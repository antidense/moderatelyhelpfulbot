from __future__ import annotations
from praw import exceptions
from praw.models import Submission

from models.reddit_models import SubmittedPost, TrackedAuthor, \
    TrackedSubreddit, ActionedComments

from settings import BOT_OWNER
from static import *
from workingdata import WorkingData


def get_age(input_text):
    matches = re.search(ASL_REGEX, input_text)
    age = -1
    if matches:
        if matches.group('age'):
            age = int(matches.group('age'))
        if matches.group('age2'):
            age = int(matches.group('age2'))
    else:
        matches = re.match(r'(?P<age>[0-9]{2})', input_text[0:2])
        if matches:
            if matches.group('age'):
                age = int(matches.group('age'))
        else:
            matches = re.match(r"[iI]((')|( a))?m (?P<age>\d{2})", input_text)
            if matches:
                if matches.group('age'):
                    age = int(matches.group('age'))
    # print(f"age: {age}  text:{input_text} ")
    return age

def check_post_nsfw_eligibility(wd: WorkingData, submitted_post):
    tr_sub: TrackedSubreddit = wd.s.query(TrackedSubreddit).get(submitted_post.subreddit_name)
    assert isinstance(tr_sub, TrackedSubreddit)
    if tr_sub and tr_sub.nsfw_pct_moderation:
        submitted_post.age = get_age(submitted_post.title)

        # Check the post author for nsfw_pct (if requested)
        post_author: TrackedAuthor = wd.s.query(TrackedAuthor).get(submitted_post.author)
        if not post_author:
            post_author = TrackedAuthor(submitted_post.author)
        assert isinstance(post_author, TrackedAuthor)
        if post_author.nsfw_pct == -1 or not post_author.last_calculated \
                or post_author.last_calculated.replace(tzinfo=timezone.utc) < \
                (datetime.now(pytz.utc) - timedelta(days=7)):
            nsfw_pct, items = post_author.calculate_nsfw(wd, instaban_subs=tr_sub.nsfw_instaban_subs)
            if hasattr(tr_sub, 'nsfw_pct_set_user_flair') and tr_sub.nsfw_pct_set_user_flair is True:
                if nsfw_pct is None or nsfw_pct < 10 and items < 10:
                    new_flair_text = f"Warning: Minimal User History"
                else:
                    new_flair_text = f"{int(nsfw_pct)}% NSFW"
                wd.s.add(post_author)
                try:
                    wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).flair.set(post_author.author_name,
                                                                               text=new_flair_text)

                    # tr_sub.get_api_handle().flair.set(post_author.author_name, text=new_flair_text)
                except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                    pass
            if hasattr(tr_sub, 'nsfw_pct_ban_duration_days') and post_author.nsfw_pct and \
                    post_author.nsfw_pct > tr_sub.nsfw_pct_threshold:
                wd.ri.mod_remove(submitted_post)
                wd.ri.send_modmail(subreddit_name=wd.bot_name,
                                   subject="[Notification] MHB post removed for high NSFW rating",
                                   body=f"post: {submitted_post.get_comments_url()} \n "
                                        f"author name: {submitted_post.author} \n"
                                        f"author activity: /u/{post_author.sub_counts} \n",
                                   use_same_thread=True)
                if tr_sub.nsfw_pct_instant_ban:
                    ban_message = NAFSC.replace("{NSFWPCT}", f"{post_author.nsfw_pct:.2f}")
                    ban_note = f"Having >80% NSFW ({post_author.nsfw_pct:.2f}%)"

                    wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                        post_author.author_name, note=ban_note, ban_message=ban_message,
                        duration=tr_sub.nsfw_pct_ban_duration_days
                    )
            if post_author.has_banned_subs_activity:
                wd.ri.mod_remove(submitted_post)
                wd.ri.send_modmail(subreddit_name=wd.bot_name,
                    subject="[Notification] MHB post removed for  banned subs",
                    body=f"post: {submitted_post.get_comments_url()} \n "
                         f"author name: {submitted_post.author} \n"
                         f"author activity: {post_author.sub_counts} \n"
                )

                ban_message = "Your account is in violation of rule #11: " \
                              " https://www.reddit.com/r/Needafriend/about/rules/. \n\n" \
                              f"Your activity: {post_author.sub_counts}. \n\n" \
                              "To keep this sub as family-friendly as possible, we temporarily restrict accounts " \
                              "that have activity on certain NSFW and dating subs. " \
                              "If this ban is in error, please contact the moderators."
                ban_note = f"Banned subs activity"
                wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                    post_author.author_name, note=ban_note, ban_message=ban_message,
                    duration=tr_sub.nsfw_pct_ban_duration_days)
            wd.s.add(post_author)

        if 25 > submitted_post.age > 12:
            submitted_post.post_flair = "strict sfw"


def nsfw_checking(wd: WorkingData):  # Does not expand comments

    posts_to_check = wd.s.query(SubmittedPost).filter(
        SubmittedPost.post_flair.ilike("%strict sfw%"),
        SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(hours=36),
        SubmittedPost.counted_status_enum
        .in_((CountedStatus.NEEDS_UPDATE, CountedStatus.NOT_CHKD, CountedStatus.PREV_EXEMPT, CountedStatus.REVIEWED))) \
        .order_by(desc(SubmittedPost.time_utc)) \
        .all()

    # .filter(or_(SubmittedPost.nsfw_last_checked < datetime.now(pytz.utc) - timedelta(hours=5),
    #    SubmittedPost.nsfw_last_checked == False)) \
    # send_modmail(tr_sub, "\n\n".join(response_lines), recent_post=recent_post, prev_post=possible_repost)
    # or_(SubmittedPost.nsfw_last_checked < datetime.now(pytz.utc) - timedelta(hours=1),
    #    SubmittedPost.nsfw_last_checked == False),

    author_list = dict()
    tick = datetime.now()

    for post in posts_to_check:
        assert isinstance(post, SubmittedPost)
        op_age = get_age(post.title)

        if post.author.lower() in NSFW_SKIP_USERS or post.author == "AutoModerator":
            wd.s.add(post)
            wd.s.commit()
            continue

        tock = datetime.now()
        if tock - tick > timedelta(minutes=3):
            print("NC: Taking too long, will break for now")
            break

        if op_age < 10:
            print(f"\tage invalid: {op_age}")
            continue
        dms_disabled = "not checked"

        warning_message = "You have specified your age as being younger than 18 or enabled 'strict sfw' mode. " \
                          "The subreddit mods will screen for potential sexual predators and block posts, however " \
                          "for this to work, you will have to temporarily disable chat requests and private messages. "\
                          "You can do that here: https://www.reddit.com/settings/messaging . " \
                          "Please see https://www.reddit.com/r/NewToReddit/wiki/" \
                          "ntr-guidetoreddit#wiki_part_7.3A__safety_on_reddit for more safety tips."
        sticky_post = "This post has been flaired 'strict sfw'. " \
                      "This means that any user who comments here or contacts the poster " \
                      "may be permanently banned from the subreddit if they have a history of NSFW comments " \
                      "or are using new throwaway accounts. This subreddit is strictly for platonic friendships, and " \
                      "the mods will not tolerate the solicitation of minors or otherwise unwanted harassment. " \
                      "The moderators will still evaluate all bans on a case by case basis prior to action taken."
        if post.time_utc.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc) - timedelta(hours=3) \
                and op_age < 18 and post.nsfw_repliers_checked is False:
            try:
                # comment_reply = post.get_api_handle().reply(sticky_post)
                # comment_reply.mod.distinguish()
                # comment_reply.mod.approve()
                pass
            except (praw.exceptions.APIException, prawcore.exceptions.Forbidden,):
                pass
            try:
                wd.ri.send_message(redditor=post.author, subject="Strict SFW Mode", message=warning_message)

                dms_disabled = "NOT DISABLED"
            except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                dms_disabled = "disabled"

            post.nsfw_repliers_checked = True
            wd.s.add(post)
        if post.subreddit_name not in wd.nsfw_monitoring_subs:
            print(f"couldn't find this sub as a sub to monitor? {post.subreddit_name}")
            continue
        tr_sub = wd.nsfw_monitoring_subs[post.subreddit_name]

        assert isinstance(tr_sub, TrackedSubreddit)

        # if 'NOT' in dms_disabled and op_age<15:
        #     tr_sub.send_modmail(body=f"Poster is <15 does NOT have PMs disabled. Remove post?  {post.get_url()}")

        time_since: timedelta = datetime.now(pytz.utc) - post.nsfw_last_checked.replace(tzinfo=timezone.utc)
        time_since_hrs = int(time_since.total_seconds() / 3600)
        # longer and longer between checks.

        if post.nsfw_last_checked.replace(tzinfo=timezone.utc) < datetime.now(pytz.utc) - timedelta(
                hours=int(time_since_hrs * 0.5 * time_since_hrs)):
            # print("checked recently...")
            continue
        print(
            f"checking post: {post.subreddit_name} {post.title} {post.time_utc} "
            f"{post.get_comments_url()} {post.post_flair} {dms_disabled}")
        top_level_comments: List[praw.models.Comment] = list(
            wd.ri.get_submission_api_handle(post).comments)

        for c in top_level_comments:
            author = None
            author_name = None

            if hasattr(c, 'author') and c.author and hasattr(c.author, 'name'):
                author_name = c.author.name
                if author_name in tr_sub.mod_list:
                    continue
                if author_name in author_list:
                    continue
                if author_name == "AutoModerator":
                    continue
                if post.author.lower() in NSFW_SKIP_USERS:
                    continue

                author: TrackedAuthor | None = wd.s.query(TrackedAuthor).get(c.author.name)
                if not author:
                    author = TrackedAuthor(c.author.name)

            if author:
                author_list[author_name] = author

                if author.nsfw_pct == -1 or not author.last_calculated \
                        or author.last_calculated.replace(tzinfo=timezone.utc) < \
                        (datetime.now(pytz.utc) - timedelta(days=7)):
                    nsfw_pct, items = author.calculate_nsfw(wd, instaban_subs=tr_sub.nsfw_instaban_subs)
                    if hasattr(tr_sub, 'nsfw_pct_set_user_flair') and tr_sub.nsfw_pct_set_user_flair is True:
                        if nsfw_pct < 10 and items < 10:
                            new_flair_text = f"Warning: Minimal User History"
                        else:
                            new_flair_text = f"{int(nsfw_pct)}% NSFW"
                        wd.s.add(author)
                        try:
                            wd.ri.get_subreddit_api_handle(tr_sub).flair.set(author_name, text=new_flair_text)
                        except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                            pass

                # tr_sub = TrackedSubreddit.get_subreddit_by_name('needafriend')
                # assert isinstance(tr_sub, TrackedSubreddit)
                if hasattr(author, 'has_banned_subs_activity') and author.has_banned_subs_activity and op_age < 18:
                    ban_note = f"ModhelpfulBot: activity on watched sub \n\n {author.sub_counts}"

                    # tr_sub.get_api_handle().banned.add(
                    #     self.author_name, ban_note=ban_note, ban_message=ban_note)
                    wd.ri.send_modmail(body=ban_note, subreddit=tr_sub, use_same_thread=True)

                if not check_actioned(wd, f"comment-{c.id}") and (
                        (author.nsfw_pct > 80 or (op_age < 18 < author.age and author.age)
                         or (op_age < 18 and author.nsfw_pct > 10))):
                    sub_counts = author.sub_counts if hasattr(author, 'sub_counts') else None

                    if tr_sub.nsfw_pct_moderation and (
                            tr_sub.nsfw_pct_instant_ban and tr_sub.nsfw_pct_ban_duration_days
                    ) and author.nsfw_pct > tr_sub.nsfw_pct_threshold:
                        # NSFWPCT: int = author.nsfw_pct
                        ban_message = NAFSC.replace("{NSFWPCT}", f"{author.nsfw_pct:.2f}")
                        ban_note = f"Having >80% NSFW ({author.nsfw_pct:.2f}%)"
                        wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                            author_name, note=ban_note, ban_message=ban_message,
                            duration=tr_sub.nsfw_pct_ban_duration_days
                        )

                    if tr_sub.modmail_receive_potential_predator_modmail:
                        comment_url = f"https://www.reddit.com/r/{post.subreddit_name}/comments/{post.id}/perma/{c.id}"
                        smart_link = f"https://old.reddit.com/message/compose?to={wd.bot_name}" \
                                     f"&subject={post.subreddit_name}" \
                                     f"&message="

                        has_bs_activity = author.has_banned_subs_activity \
                            if hasattr(author, 'has_banned_subs_activity') else "unknown"

                        if has_bs_activity and author.nsfw_pct and author.nsfw_pct > tr_sub.nsfw_pct_threshold:
                            ban_note = f"{author.author_name} has activity on watched sub \n\n " \
                                       f"{author.sub_counts} and was banned"

                            wd.ri.get_subreddit_api_handle(tr_sub).banned.add(
                                author.author_name, note="activity on banned subs", ban_message=NAFBS,
                                duration=tr_sub.nsfw_pct_ban_duration_days)
                            wd.ri.send_modmail(body=ban_note, subreddit=tr_sub, use_same_thread=False)

                        else:
                            ban_mc_link = f"{smart_link}$ban {author_name} 999 {NAFMC}".replace(" ", "%20")
                            ban_sc_link = f"{smart_link}$ban {author_name} 30 {NAFSC}".replace("{NSFWPCT}",
                                                                                               str(int(
                                                                                                   author.nsfw_pct)))
                            ban_cf_link = f"{smart_link}$ban {author_name} 999 {NAFCF}"
                            ban_bs_link = f"{smart_link}$ban {author_name} 30 {NAFBS}"

                            response = f"Author very nsfw: http://www.reddit.com/u/{author_name} . " \
                                       f"Commented on: {post.get_comments_url()} \n\n. " \
                                       f"Link to comment: {comment_url} \n\n. " \
                                       f"Has activity on banned subs?: {has_bs_activity} \n\n. " \
                                       f"Poster's age {op_age}. Commenter's age {author.age} \n\n" \
                                       f"Has nsfw post? {author.has_nsfw_post} \n\n" \
                                       f"Comment text: {c.body} \n\n" \
                                       f"Sub activity: {sub_counts} \n\n" \
                                       f"[$ban-sc (ban for sexual content)]({ban_sc_link}) | " \
                                       f"[$ban-mc (ban for minor contact)]({ban_mc_link}) | " \
                                       f"[$ban-sb (ban for subreddit history)]({ban_bs_link}) | " \
                                       f"[$ban-cf (ban for catfishing)]({ban_cf_link}) | "

                            subject = f"[Notification] Found this potential predator {author_name} " \
                                      f"score={int(author.nsfw_pct)}"
                            print(response)
                            try:
                                c.mod.remove()
                            except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                                pass
                            wd.ri.send_modmail(subject=subject, body=response, subreddit=tr_sub, use_same_thread=True)

                            #wd.ri.reddit_client.redditor(BOT_OWNER).message(subject, response)
                    record_actioned(wd, f"comment-{c.id}")
        post.nsfw_repliers_checked = True
        post.nsfw_last_checked = datetime.now(pytz.utc)
        wd.s.add(post)
        wd.s.commit()


def check_actioned(wd: WorkingData, comment_id: str):
    response: ActionedComments = wd.s.query(ActionedComments).get(comment_id)
    if response:
        return True
    return False


def record_actioned(wd: WorkingData, comment_id: str):
    response: ActionedComments = wd.s.query(ActionedComments).get(comment_id)
    if response:
        return
    wd.s.add(ActionedComments(comment_id))
