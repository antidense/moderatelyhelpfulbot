#!/usr/bin/env python3.7
from __future__ import annotations

from praw import exceptions
from praw.models import Submission
from static import *
from datetime import datetime, timedelta, timezone
from typing import List
import praw
import prawcore
import pytz
import yaml
from settings import  MAIN_BOT_NAME, ACCEPTING_NEW_SUBS, BOT_OWNER
"""
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from praw.models.listing.generator import ListingGenerator
from praw.models.listing.mixins.redditor import SubListing
from sqlalchemy.orm import sessionmaker
from enum import Enum  # has to be at the end?
import queue
"""

from utils import look_for_rule_violations3, get_age, get_subreddit_by_name
from models.reddit_models.redditinterface import SubredditInfo
# from modmail import handle_modmail_messages, handle_direct_messages
from models.reddit_models import ActionedComments, CommonPost, Stats2, SubAuthor, SubmittedPost, TrackedAuthor, \
    TrackedSubreddit, RedditInterface
from logger import logger
from core import dbobj


dbobj.load_models()

"""
To do list:
asyncio 
incorporate toolbox? https://www.reddit.com/r/nostalgia/wiki/edit/toolbox check user notes?
active status to an ENUM
add non-binary gender
"""
BOT_NAME = None
from workingdata import WorkingData

def main_loop():
    wd = WorkingData()
    wd.s = dbobj.s
    wd.ri = RedditInterface()
    global BOT_NAME
    BOT_NAME  = wd.ri.reddit_client.user.me().name
    print(f"My name is {wd.ri.reddit_client.user.me()}, {BOT_NAME}")
    # load_settings(wd)
    sub_info = wd.ri.get_subreddit_info(subreddit_name=MAIN_BOT_NAME)
    wd.ri.bot_sub : TrackedSubreddit = wd.s.query(TrackedSubreddit).get(MAIN_BOT_NAME)
    if not wd.ri.bot_sub:
        wd.ri.bot_sub = TrackedSubreddit(subreddit_name=MAIN_BOT_NAME, sub_info=sub_info)
    if not wd.ri.bot_sub.mm_convo_id:
        wd.ri.bot_sub.mm_convo_id = wd.ri.get_modmail_thread_id(subreddit_name=MAIN_BOT_NAME)
    wd.s.add(wd.ri.bot_sub)
    wd.s.commit()


    i = 0
    # update_sub_list(intensity=2)
    purge_old_records(wd)

    while True:
        i += 1
        print('start_loop')

        intensity = 1 if (i - 1) % 15 == 0 else 0

        # First: Update sub list:
        if wd.to_update_list or len(wd.sub_list) == 0 or i % 50 == 0:
            print("updating list")
            update_sub_list(wd, intensity=intensity)
            wd.to_update_list = False

        try:
            look_for_rule_violations3(wd)
            # Gather posts
            chunk_size = 200 if intensity == 1 else 300
            chunked_list = [wd.sub_list[j:j + chunk_size] for j in range(0, len(wd.sub_list), chunk_size)]

            updated_subs = []

            for sub_list in chunked_list:
                sub_list_str = "+".join(sub_list)
                print(len(sub_list_str), sub_list_str)
                updated_subs += check_new_submissions(wd, sub_list=sub_list_str, intensity=intensity)
                check_spam_submissions(wd, sub_list=sub_list_str, intensity=intensity)

            if intensity == 1:
                updated_subs = None

            start = datetime.now(pytz.utc)
            if i % 15 == 0:
                intensity = 5
            look_for_rule_violations3(wd)  # uses a lot of resource
            # look_for_rule_violations2(wd, intensity=intensity, sub_list=sub_list)  # uses a lot of resource
            print("$$$checking rule violations took this long", datetime.now(pytz.utc) - start)

            if i % 75 == 0:
                purge_old_records(wd)

            if (i - 1) % 300 == 0:
                print("$updating top posts", datetime.now(pytz.utc) - start)
                # update_common_posts('nostalgia')
                # update_common_posts('homeimprovement')

            if i % 300 == 0:
                print("$updating top posts", datetime.now(pytz.utc) - start)
                # update_common_posts('nostalgia')

            # if i % 70 == 0:
            #    print("$Looking for bot spam posts", datetime.now(pytz.utc) - start)
            #    check_common_posts(['nostalgia'])

            # update_TMBR_submissions(look_back=timedelta(days=7))
            #  do_automated_replies()  This is currently disabled!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            handle_direct_messages(wd)
            handle_modmail_messages(wd)

            nsfw_checking(wd)
            if (i - 1) % 15 == 0:
                calculate_stats(wd)

        except (prawcore.exceptions.ServerError, prawcore.exceptions.ResponseException):
            import time
            print("sleeping due to server error")
            time.sleep(60 * 5)  # sleep for a bit server errors
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            print(trace)
            wd.ri.send_modmail(subreddit_name=BOT_NAME, subject="[Notification] MHB Exception", body=trace,
                               use_same_thread=True)
            wd.s.add(wd.ri.bot_sub)
            wd.s.commit()

"""
def load_settings(wd: WorkingData):  # Not being used
    global RESPONSE_TAIL
    global MAIN_SETTINGS
    wiki_settings = wd.ri.get_subreddit_str_api_handle(BOT_NAME).wiki['moderatelyhelpfulbot']
    MAIN_SETTINGS = yaml.safe_load(wiki_settings.content_md)

    if 'response_tail' in MAIN_SETTINGS:
        RESPONSE_TAIL = MAIN_SETTINGS['response_tail']
    # load_subs(main_settings)
"""

def update_sub_list(wd: WorkingData, intensity=0):
    print('updating subs..', sep="")
    wd.sub_list = []

    if intensity > 4:
        trs = wd.s.query(TrackedSubreddit).filter(TrackedSubreddit.active_status > 0).all()  # sql query
        for tr in trs:
            assert isinstance(tr, TrackedSubreddit)
            active_status, error = tr.update_from_subinfo(wd.ri.get_subreddit_info(tr))

            print(f"Checked {tr.subreddit_name}:\t{tr.active_status}\t{error}")
            if not tr.mm_convo_id and tr.active_status > 0:
                tr.checking_mail_enabled = True
                try:
                    tr.mm_convo_id = wd.ri.get_modmail_thread_id(subreddit_name=tr.subreddit_name)

                except prawcore.exceptions.Forbidden:
                    tr.checking_mail_enabled = False
                if tr.mm_convo_id:
                    print(f"found convo id: {tr.mm_convo_id}")
                wd.s.add(tr)
                wd.s.commit()
    trs = wd.s.query(TrackedSubreddit).filter(TrackedSubreddit.active_status > 0).all()
    for tr in trs:
        # print(f'updating /r/{tr.subreddit_name}', end="")
        assert isinstance(tr, TrackedSubreddit)

        wd.sub_list.append(tr.subreddit_name)

        if tr.subreddit_name not in wd.sub_dict:
            worked, status = tr.reload_yaml_settings()
            wd.sub_dict[tr.subreddit_name] = tr
            # print(f"---- {worked}, {status}")
            if not tr.mod_list:
                try:
                    tr.mod_list = str(wd.ri.get_mod_list(tr.subreddit_name))
                except prawcore.exceptions.Forbidden:
                    tr.active_status = SubStatus.SUB_GONE.value

                # print(tr.mod_list)
                wd.s.add(tr)

        if tr.subreddit_name not in wd.sub_dict:
            if tr.last_updated < datetime.now() - timedelta(days=7) and tr.active_status >= 0:
                print(f'...rechecking...{tr.last_updated}')
                sub_info = wd.ri.get_subreddit_info(tr.subreddit_name)
                tr.update_from_subinfo(sub_info)
                tr.reload_yaml_settings()
                wd.s.add(tr)
                wd.s.commit()
            else:
                tr.reload_yaml_settings()
                print(f'')

        wd.sub_dict[tr.subreddit_name] = tr

    wd.s.commit()
    return


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
    # wd.s.commit()


def purge_old_records(wd: WorkingData):  # requires db only
    purge_statement = "delete t  from RedditPost t inner join TrackedSubs s on t.subreddit_name = s.subreddit_name where  t.time_utc  < utc_timestamp() - INTERVAL greatest(s.min_post_interval_mins, 60*24*10) MINUTE  and t.flagged_duplicate=0 and t.pre_duplicate=0"
    _ = wd.s.execute(purge_statement)


def purge_old_records_by_subreddit(wd: WorkingData, tr_sub: TrackedSubreddit):
    print("looking for old records to purge from ", tr_sub.subreddit_name, tr_sub.min_post_interval)
    _ = wd.s.query(SubmittedPost).filter(
        SubmittedPost.time_utc < datetime.now(pytz.utc).replace(tzinfo=None) - tr_sub.min_post_interval,
        SubmittedPost.counted_status.not_in([CountedStatus.FLAGGED.value,
                                             CountedStatus.REMOVED.value, CountedStatus.BLKLIST]),
        SubmittedPost.pre_duplicate.is_(False),
        SubmittedPost.subreddit_name == tr_sub.subreddit_name).delete()

    # SubmittedPost.flagged_duplicate.is_(False),
    # print("purging {} old records from {}", len(to_delete), tr_sub.subreddit_name)
    # to_delete.delete()
    wd.s.commit()


def check_new_submissions(wd: WorkingData, query_limit=800, sub_list='mod', intensity=0):
    subreddit_names = []
    subreddit_names_complete = []
    logger.info(f"pulling new posts!  intensity: {intensity}")

    possible_new_posts = [a for a in wd.ri.reddit_client.subreddit(sub_list).new(limit=query_limit)]

    count = 0
    for post_to_review in possible_new_posts:

        subreddit_name = str(post_to_review.subreddit).lower()
        if intensity == 0 and subreddit_name in subreddit_names_complete:
            # print(f'done w/ {subreddit_name}')
            continue
        previous_post: SubmittedPost = wd.s.query(SubmittedPost).get(post_to_review.id)
        if previous_post:
            subreddit_names_complete.append(subreddit_name)
            continue
        if not previous_post:
            post = SubmittedPost(post_to_review)
            if post.subreddit_name in ("needafriend", "makenewfriendshere"):
                check_post_nsfw_eligibility(wd, post)
            if subreddit_name not in subreddit_names:
                subreddit_names.append(subreddit_name)

            wd.s.add(post)
            count += 1
    logger.info(f'found {count} posts')
    logger.debug("updating database...")
    wd.s.commit()
    return subreddit_names




def check_spam_submissions(wd: WorkingData, sub_list='mod', intensity=0):
    possible_spam_posts = []
    try:
        possible_spam_posts = [a for a in wd.ri.reddit_client.subreddit(sub_list).mod.spam(only='submissions')]
    except prawcore.exceptions.Forbidden:
        pass
    for post_to_review in possible_spam_posts:
        previous_post: SubmittedPost = wd.s.query(SubmittedPost).get(post_to_review.id)
        if previous_post and intensity == 0:
            break
        if not previous_post:
            post = SubmittedPost(post_to_review)
            post.posted_status=PostedStatus.SPAM_FLT.value
            post.reviewed = True
            sub_list = post.subreddit_name.lower()
            # logger.info("found spam post: '{0}...' http://redd.it/{1} ({2})".format(post.title[0:20], post.id,
            #                                                                         subreddit_name))

            # post.reviewed = True
            wd.s.add(post)
            subreddit_author: SubAuthor = wd.s.query(SubAuthor).get((sub_list, post.author))
            if subreddit_author and subreddit_author.hall_pass >= 1:
                subreddit_author.hall_pass -= 1
                post.api_handle.mod.approve()
                wd.s.add(subreddit_author)
    wd.s.commit()


def calculate_stats(wd: WorkingData):
    # Todo: repeat offenders?

    statement = 'select count(*),counted_status, subreddit_name, date(time_utc) as date from RedditPost  where   time_utc < date(utc_timestamp) group by date(time_utc),  subreddit_name,  counted_status order by date desc'
    rs = wd.s.execute(statement)

    for row in rs:
        count = row[0]
        counted_status = row[1]
        subreddit_name = row[2]
        date = row[3]
        stat_name = str(CountedStatus(counted_status)).replace("CountedStatus.", "").lower()
        sub_stat = wd.s.query(Stats2).get((subreddit_name, date, stat_name))
        if not sub_stat:
            sub_stat = Stats2(subreddit_name, date, stat_name)
            sub_stat.value_int = count
            wd.s.add(sub_stat)
        else:
            break
    wd.s.commit()

    statement = 'select count(*), sum(if(counted_status=5, 1, 0)) as flagged, sum(if(counted_status=3, 1, 0)) as blacklisted, sum(if(counted_status=20, 1, 0)) as removed,  subreddit_name, date(time_utc) as date from RedditPost  where  time_utc > utc_timestamp() - INTERVAL  60*24*14 MINUTE and time_utc < date(utc_timestamp)  group by  subreddit_name, date  order by date'
    rs = wd.s.execute(statement)
    for row in rs:
        count = row[0]
        flagged_count = row[1]
        blacklisted_count = row[2]
        removed_count = row[3]
        subreddit_name = row[4]
        date = row[5]
        sub_stat = wd.s.query(Stats2).get((subreddit_name, date, 'collected'))
        if not sub_stat:
            sub_stat = Stats2(subreddit_name, date, 'collected')
            sub_stat.value_int = count
            wd.s.add(sub_stat)
        sub_stat = wd.s.query(Stats2).get((subreddit_name, date, 'flagged'))
        if not sub_stat:
            sub_stat = Stats2(subreddit_name, date, 'flagged')
            sub_stat.value_int = flagged_count
            wd.s.add(sub_stat)
            sub_stat = Stats2(subreddit_name, date, 'blacklisted')
            sub_stat.value_int = blacklisted_count
            wd.s.add(sub_stat)
            sub_stat = Stats2(subreddit_name, date, 'removed')
            sub_stat.value_int = removed_count
            wd.s.add(sub_stat)
        sub_stat = wd.s.query(Stats2).get((subreddit_name, date, 'flagged_total'))
        if not sub_stat:
            sub_stat = Stats2(subreddit_name, date, 'flagged_total')
            sub_stat.value_int = flagged_count + blacklisted_count + removed_count
            wd.s.add(sub_stat)

    # REMOVED(20) added as of 10/27/21 - previously not tracked separately from FLAGGED

    statement = 'select count(*), avg(time_to_sec(timediff(response_time, time_utc))) as latency, subreddit_name, date(time_utc) as date from RedditPost  where  time_utc > utc_timestamp() - INTERVAL  60*24*14 MINUTE and time_utc < date(utc_timestamp)  and response_time is not null group by  subreddit_name, date  order by date'
    rs = wd.s.execute(statement)
    for row in rs:
        _ = row[0]
        latency = row[1]
        subreddit_name = row[2]
        date = row[3]
        sub_stat2 = wd.s.query(Stats2).get((subreddit_name, date, 'latency'))
        if not sub_stat2:
            sub_stat2 = Stats2(subreddit_name, date, 'latency')
            sub_stat2.value_int = int(latency)
            wd.s.add(sub_stat2)
            sub_stat3 = Stats2(subreddit_name, date, 'latency_ct')
            sub_stat3.value_int = row[0]
            wd.s.add(sub_stat3)

    wd.s.commit()
    wd.s.execute(statement)


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
                if nsfw_pct < 10 and items < 10:
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
                wd.ri.send_modmail(subreddit_name=BOT_NAME,
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
                wd.ri.send_modmail(subreddit_name=BOT_NAME,
                    subject="[Notification] MHB post removed for  banned subs",
                    body=f"post: {submitted_post.get_comments_url()} \n "
                         f"author name: {submitted_post.author} \n"
                         f"author activity: {post_author.sub_counts} \n"
                )

                ban_message = "Your account is in violation of rule #11: " \
                              " https://www.reddit.com/r/Needafriend/about/rules/. \n\n" \
                              f"Your activity: {post_author.sub_counts}. \n\n" \
                              "To keep this sub as family-friendly as possible, we temporarily restrict accounts " \
                              "that have activity on certain NSFW subs. " \
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
        SubmittedPost.counted_status < 3) \
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
            print("Taking too long, will break for now")
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
        tr_sub = get_subreddit_by_name(wd, post.subreddit_name, create_if_not_exist=False, update_if_due=False)
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
                        smart_link = f"https://old.reddit.com/message/compose?to={BOT_NAME}" \
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
                            wd.ri.send_modmail(body=ban_note, subreddit=tr_sub, use_same_thread=True)

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

                            wd.ri.reddit_client.redditor(BOT_OWNER).message(subject, response)
                    record_actioned(wd, f"comment-{c.id}")
        post.nsfw_repliers_checked = True
        post.nsfw_last_checked = datetime.now(pytz.utc)
        wd.s.add(post)
        wd.s.commit()


def update_common_posts(wd: WorkingData, subreddit_name, limit=1000):
    top_posts = [a for a in wd.ri.reddit_client.subreddit(subreddit_name).top(limit=limit)]
    count = 0
    for post_to_review in top_posts:
        previous_post: CommonPost = wd.s.query(CommonPost).get(post_to_review.id)
        if not previous_post:
            post = CommonPost(post_to_review)
            wd.s.add(post)
            count += 1
    logger.info(f'found {count} top posts')
    logger.debug("updating database...")
    wd.s.commit()


def check_common_posts(wd: WorkingData, subreddit_names):
    # bot spam
    sub_list = str(subreddit_names).replace("[", "(").replace("]", ")")
    statement = f"select c.title, c.id, r.id, r.subreddit_name from CommonPosts c left join RedditPost r on c.title = r.title where r.subreddit_name in {sub_list} and r.id !=c.id and r.time_utc> utc_timestamp() - Interval 1 day and r.counted_status != 30;"
    blurbs = {}
    print(statement)
    rs = wd.s.execute(statement)

    for row in rs:
        cp: CommonPost = wd.s.query(CommonPost).get(row[1])
        rp: SubmittedPost = wd.s.query(SubmittedPost).get(row[2])

        blurb = f"|{rp.title}" \
                f"|[{cp.id}]({cp.get_comments_url()})" \
                f"|[{cp.author}](/u/{cp.author})" \
                f"|[{rp.id}]({rp.get_comments_url()})" \
                f"|[{rp.author}](/u/{rp.author})" \
                f"|\n"
        post_subreddit_name = row[3]

        if post_subreddit_name not in blurbs:
            blurbs[post_subreddit_name] = [
                "I found the following potential botspam"
                "\n\n|Original Title|Orig Post ID|Original Author|RepeatPostID|Repost Author|\n"
                "|:---|:-------|:------|:-----------|:------|\n"]

        blurbs[post_subreddit_name] += blurb
        print(blurb)

        # rp.mod_remove()
        rp.counted_status = CountedStatus.BOT_SPAM.value
        wd.s.add(rp)

    for subreddit_name in blurbs:
        wd.ri.send_modmail(subject=f"[Notification] Post by possible karma hackers:",
                           body="".join(blurbs[subreddit_name]), subreddit=subreddit_name, use_same_thread=True)
        wd.ri.send_modmail(subject=f"[Notification] botspam notification {subreddit_name}",
                           body="".join(blurbs[subreddit_name]), subreddit_name=BOT_NAME, use_same_thread=True)

    wd.s.commit()


def handle_dm_command(wd: WorkingData, subreddit_name: str, requestor_name, command, parameters) \
        -> tuple[str, bool]:
    subreddit_name: str = subreddit_name[2:] if subreddit_name.startswith('r/') else subreddit_name
    subreddit_name: str = subreddit_name[3:] if subreddit_name.startswith('/r/') else subreddit_name
    subreddit_names = subreddit_name.split('+') if '+' in subreddit_name else [subreddit_name]  # allow

    command: str = command[1:] if command.startswith("$") else command

    tr_sub = get_subreddit_by_name(wd, subreddit_name, create_if_not_exist=True)
    if not tr_sub:
        return "Error retrieving information for /r/{}".format(subreddit_name), True
    moderators: List[str] = wd.ri.get_mod_list(subreddit=tr_sub)
    print("asking for permission: {}, mod list: {}".format(requestor_name, ",".join(moderators)))
    if requestor_name is not None and requestor_name not in moderators and requestor_name != BOT_OWNER \
            and requestor_name != "[modmail]":
        if subreddit_name == "subredditname" or subreddit_name == "yoursubredditname":
            return "Please change 'subredditname' to the name of your subreddit so I know what subreddit you mean!", \
                   True
        return f"You do not have permission to do this. Are you sure you are a moderator of {subreddit_name}?\n\n " \
               f"/r/{subreddit_name} moderator list: {str(moderators)}, your name: {requestor_name}", True

    if tr_sub.bot_mod is None and requestor_name in moderators:
        tr_sub.bot_mod = requestor_name
        wd.s.add(tr_sub)
        wd.s.commit()

    if command in ['summary', 'unban', 'hallpass', 'blacklist', 'ban', 'ban-sc', 'ban-mc', 'ban-cf']:
        author_param = parameters[0] if parameters else None

        if not author_param:
            return "No author name given", True
        author_param = author_param.lower()
        author_param = author_param.replace('/u/', '')
        author_param = author_param.replace('u/', '')
        author_handle = wd.ri.reddit_client.redditor(author_param)
        if author_handle:
            try:
                _ = author_handle.id  # force load actual username capitalization
                author_param = author_handle.name
            except (prawcore.exceptions.NotFound, AttributeError):
                pass

        if command == 'summary':
            return tr_sub.get_author_summary(wd, author_param), True

        # Need to be an actual reddit author after this point
        if not author_handle:
            return "could not find that username `{}`".format(author_param), True

        if command in ['unban', 'hallpass', 'blacklist']:
            subreddit_author: SubAuthor = wd.s.query(SubAuthor).get((subreddit_name, author_handle.name))
            if not subreddit_author:
                subreddit_author = SubAuthor(tr_sub.subreddit_name, author_handle.name)

            if command == "unban":
                if subreddit_author.next_eligible.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc):
                    subreddit_author.next_eligible = datetime(2019, 1, 1, 0, 0)
                    return_text = "User was removed from blacklist"
                else:
                    try:
                        wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).banned.remove(author_param)
                        return "Unban succeeded", False
                    except prawcore.exceptions.Forbidden:
                        return "Unban failed, I don't have permission in this subreddit to do that. Sorry.", True
                    except prawcore.exceptions.BadRequest:
                        return "The reddit server did not let me do this. Are they already unbanned?", True
            elif command == "hallpass":
                return_text = f"User {author_param} has been granted a hall pass. " \
                              "This means the next post by the user in this " \
                              "subreddit will not be automatically removed."
            elif command == "blacklist":
                subreddit_author.currently_blacklisted = True
                return_text = "User {} has been blacklisted from modmail. " \
                    .format(author_handle.name)
            else:
                return_text = "shouldn't get here?"
            wd.s.add(subreddit_author)
            return return_text, True

        ban_reason = " ".join(parameters[2:]) if parameters and len(parameters) >= 2 else None
        ban_note = "ModhelpfulBot: per modmail command"
        ban_length = int(parameters[1]) if parameters and len(parameters) >= 2 else None
        try:

            print(parameters, ban_length)
            if ban_length > 999 or ban_length < 1:
                return f"Invalid ban length: '{ban_length}'", True
        except ValueError:
            return f"Invalid ban length: '{ban_length}'", True

        try:
            if ban_length == 999 or ban_length is None:
                print("permanent ban", ban_length)
                wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                    author_param, note=ban_note, ban_message=ban_reason)
            else:
                print("non permanent ban", ban_length)
                wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                    author_param, note=ban_note, ban_message=ban_reason,
                    duration=ban_length)
            return "Ban for {} was successful".format(author_param), True
        except prawcore.exceptions.Forbidden:
            return "Ban failed, I don't have permission to do that", True
        except praw.exceptions.RedditAPIException:
            return f"Ban failed, reddit reported {author_param} was not a valid user", True
    elif command == "showrules":
        lines = ["Rules for {}:".format(subreddit_name), ]
        rules = wd.ri.reddit_client.subreddit(subreddit_name).rules()['rules']
        for count, rule in enumerate(rules):
            lines.append("{}: {}".format(count + 1, rule['short_name']))
        return "\n\n".join(lines), True
    elif command == "stats":
        return tr_sub.get_sub_stats(), True
    elif command == "approve":
        submission_id = parameters[0] if parameters else None
        if not submission_id:
            return "No submission name given", True
        submission = wd.ri.reddit_client.submission(submission_id)
        if not submission:
            return "Cannot find that submission", True
        submission.mod.approve()
        return "Submission was approved.", False
    elif command == "remove":
        submission_id = parameters[0] if parameters else None
        if not submission_id:
            return "No author name given", True
        submission = wd.ri.reddit_client.submission(submission_id)
        if not submission:
            return "Cannot find that submission", True
        submission.mod.remove()
        return "Submission was removed.", True

    elif command == "citerule" or command == "testciterule":
        if not parameters:
            return "No rule # given", True
        try:
            # converting to integer
            rule_num = int(parameters[0]) - 1
        except ValueError:
            return "invalid rule #`{}`".format(parameters[0]), True
        rules = wd.ri.reddit_client.subreddit(subreddit_name).rules()['rules']
        rule = rules[rule_num] if rule_num < len(rules) else None
        if not rule:
            return "Invalid rule", True
        reply = "Please see rule #{}:\n\n>".format(rule_num + 1) + rule['short_name'].replace("\n\n", "\n\n>")
        internal = False if command == "citerule" else True
        return reply, internal
    elif command == "citerulelong" or command == "testciterulelong":
        if not parameters:
            return "No rule # given", True
        try:
            # converting to integer
            rule_num = int(parameters[0]) - 1
        except ValueError:
            return "invalid rule #`{}`".format(parameters[0]), True
        rules = wd.ri.reddit_client.subreddit(subreddit_name).rules()['rules']
        rule = rules[rule_num] if rule_num < len(rules) else None
        if not rule:
            return "Invalid rule", True
        reply = "Please see rule #{}:\n\n>".format(rule_num + 1) + rule['description'].replace("\n\n", "\n\n>")
        internal = False if command == "citerulelong" else True
        return reply, internal
    elif command == "canned" or command == "testcanned":
        if not parameters:
            return "No canned name given", True
        print(tr_sub.canned_responses)
        if parameters[0] not in tr_sub.canned_responses:
            return "no canned response by the name `{}`".format(parameters[0]), True
        reply = tr_sub.populate_tags2(tr_sub.canned_responses[parameters[0]])
        internal = False if command == "citerule" else True
        return reply, internal
    elif command == "reset":
        author_param = parameters[0] if parameters else None
        subreddit_author: SubAuthor = wd.s.query(SubAuthor).get((tr_sub.subreddit_name, author_param))
        if subreddit_author and subreddit_author.next_eligible.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc):
            subreddit_author.next_eligible = datetime(2019, 1, 1, 0, 0)
            return "User was removed from blacklist", False
        posts = wd.s.query(SubmittedPost).filter(SubmittedPost.author == author_param,
                                                 # SubmittedPost.flagged_duplicate.is_(True),
                                                 SubmittedPost.counted_status == CountedStatus.FLAGGED.value,
                                                 SubmittedPost.subreddit_name == tr_sub.subreddit_name).all()
        for post in posts:
            post.flagged_duplicate = False
            post.counted_status = CountedStatus.EXEMPTED.value
            wd.s.add(post)
        wd.s.commit()

    elif command == "update":  # $update
        sub_info = wd.ri.get_subreddit_info(tr_sub.subreddit_name)
        tr_sub.update_from_subinfo(sub_info)
        worked, status = tr_sub.reload_yaml_settings()
        help_text = ""
        if "404" in status:
            help_text = f"This error means the wiki config page needs to be created. " \
                        f" See https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{MAIN_BOT_NAME}. "
        elif "403" in status:
            help_text = f"This error means the bot doesn't have enough permissions to view the wiki page. " \
                        f"Please make sure that you invited the bot to moderate and that the bot has " \
                        f"accepted the moderator invitation and give the bot wiki " \
                        f"privileges here: https://www.reddit.com/r/{tr_sub.subreddit_name}/about/moderators/ . " \
                        f"It is possible that the bot has not accepted the invitation due to current load.  " \
                        f"Link to your config: https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{MAIN_BOT_NAME}. "
        elif "yaml" in status:
            help_text = "Looks like there is an error in your yaml code. " \
                        "Please make sure to validate your syntax at https://yamlvalidator.com/.  " \
                        f"Link to your config: https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{MAIN_BOT_NAME}. "
        elif "single document in the stream" in status:
            help_text = "Looks like there is an extra double hyphen in your code at the end, e.g. '--'. " \
                        "Please remove it.  " \
                        f"Link to your config: https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{MAIN_BOT_NAME}. "

        sub_status_code = tr_sub.active_status
        sub_status_enum = str(SubStatus(sub_status_code))

        reply_text = f"Received message to update config for {subreddit_name}.  See the output below. {status}" \
                     f"Please message [/r/moderatelyhelpfulbot](https://www.reddit.com/" \
                     f"message/old?to=%2Fr%2Fmoderatelyhelpfulbot) if you have any questions \n\n" \
                     f"Update report: \n\n >{help_text}" \
                     f"\n\nCurrent Status: {sub_status_code}: {sub_status_enum}  "
        bot_owner_message = f"subreddit: {subreddit_name}\n\nrequestor: {requestor_name}\n\n" \
                            f"report: {status}\n\nCurrent Status: {sub_status_code}: {sub_status_enum}  "
        # wd.ri.reddit_client.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
        try:
            assert isinstance(requestor_name, str)
        except AssertionError:
            wd.ri.send_modmail(subreddit_name=BOT_NAME,body="Invalid user: bot_owner_message", use_same_thread=True)
            return "bad requestor", True
        if requestor_name and requestor_name.lower() != BOT_OWNER.lower():
            wd.ri.send_modmail(subreddit_name=BOT_NAME, body=bot_owner_message, use_same_thread=True)
        wd.s.add(tr_sub)
        wd.s.commit()
        wd.to_update_list = True
        return reply_text, True
    else:
        return "I did not understand that command", True


def handle_direct_messages(wd: WorkingData):

    for message in wd.ri.reddit_client.inbox.unread(limit=None):
        logger.info("got this email author:{} subj:{}  body:{} ".format(message.author, message.subject, message.body))

        # Get author name, message_id if available
        requestor_name = message.author.name if message.author else None

        message_id = wd.ri.reddit_client.comment(message.id).link_id if message.was_comment else message.name
        body_parts = message.body.split(' ')
        command = body_parts[0].lower() if len(body_parts) > 0 else None
        # subreddit_name = message.subject.replace("re: ", "") if command else None
        # First check if already actioned
        if check_actioned(wd, message_id):
            message.mark_read()  # should have already been "read"
            continue
        # Check if this a user mention (just ignore this)
        elif message.subject.startswith('username mention'):
            message.mark_read()
            continue
        # Check if this a user mention (just ignore this)
        elif message.subject.startswith('moderator added'):
            message.mark_read()
            continue
        # Check if this is a ban notice (not new modmail)
        elif message.subject.startswith("re: You've been temporarily banned from participating"):
            message.mark_read()
            subreddit_name = message.subject.replace("re: You've been temporarily banned from participating in r/", "")
            if not check_actioned(wd, "ban_note: {0}".format(requestor_name)):
                # record actioned first out of safety in case of error
                record_actioned(wd, "ban_note: {0}".format(message.author))

                tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
                if tr_sub and tr_sub.modmail_posts_reply and message.author:
                    try:
                        message.reply(body=tr_sub.get_author_summary(wd, message.author.name))
                    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                        pass
        # Respond to an invitation to moderate
        elif message.subject.startswith('invitation to moderate'):
            mod_mail_invitation_to_moderate(wd, message)
        elif command in ("summary", "update", "stats") or command.startswith("$"):
            if requestor_name is None:
                print("requestor name is none?")
                continue
            subject_parts = message.subject.replace("re: ", "").split(":")
            thread_id = subject_parts[1] if len(subject_parts) > 1 else None
            subreddit_name = subject_parts[0].lower().replace("re: ", "").replace("/r/", "").replace("r/", "")
            tr_sub = get_subreddit_by_name(wd, subreddit_name)
            response, _ = handle_dm_command(wd, subreddit_name, requestor_name, command, body_parts[1:])
            if tr_sub and thread_id:
                wd.ri.send_modmail(subreddit_name=tr_sub, body=response[:9999], thread_id=thread_id)
            else:
                message.reply(body=response[:9999])
            bot_owner_message = f"subreddit: {subreddit_name}\n\n" \
                                f"requestor: {requestor_name}\n\n" \
                                f"command: {command}\n\n" \
                                f"response: {response}\n\n" \
                                f"wiki: https://www.reddit.com/r/{subreddit_name}/wiki/{BOT_NAME}\n\n"
            if requestor_name.lower() != BOT_OWNER.lower():
                wd.ri.send_modmail(subreddit_name=BOT_NAME, subject="[Notification]  Command processed",
                                   body=bot_owner_message, use_same_thread=True)
            # wd.ri.reddit_client.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)

        elif requestor_name and not check_actioned(wd, requestor_name):
            record_actioned(wd, requestor_name)
            message.mark_read()
            try:
                # ignore profanity
                if "fuck" in message.body:
                    continue
                message.reply(body="Hi, thank you for messaging me! "
                                   "I am a non-sentient bot, and I act only in the accordance of the rules set by the "
                                   "moderators "
                                   "of the subreddit. Unfortunately, I am unable to answer or direct requests. Please "
                                   "see this [link](https://www.reddit.com/r/SolariaHues/comments/mz7zdp/)")
                import pprint

                # assume you have a Reddit instance bound to variable `reddit`

                # print(submission.title)  # to make it non-lazy
                pprint.pprint(vars(message))
            except prawcore.exceptions.Forbidden:
                pass
            except praw.exceptions.APIException:
                pass

        message.mark_read()
        record_actioned(wd, message_id)
    wd.s.commit()


def mod_mail_invitation_to_moderate(wd: WorkingData, message):
    subreddit_name = message.subject.replace("invitation to moderate /r/", "")
    tr_sub = get_subreddit_by_name(wd, subreddit_name, create_if_not_exist=False)

    # accept invite if accepting invites or had been accepted previously
    print("to make sub:", subreddit_name)
    if ACCEPTING_NEW_SUBS or tr_sub and 'karma' not in subreddit_name.lower():
        if not tr_sub:
            tr_sub = get_subreddit_by_name(wd, subreddit_name, create_if_not_exist=True)

        try:
            sub_api_handle = wd.ri.get_subreddit_api_handle(tr_sub)
            sub_api_handle.mod.accept_invite()
        except praw.exceptions.APIException:
            message.reply(body="Error: Invite message has been rescinded? or already accepted?")
            message.mark_read()

        message.reply(
            body=f"Hi, thank you for inviting me!  I will start working now. Please make sure I have a config. "
                 f"I will try to create one at https://www.reddit.com/r/{subreddit_name}/wiki/{BOT_NAME} . "
                 f"You may need to create it. You can find examples at "
                 f"https://www.reddit.com/r/{BOT_NAME}/wiki/index . ")
        try:
            access_status = wd.ri.check_access(tr_sub, ignore_no_mod_access=True)
            if access_status == SubStatus.NO_CONFIG:
                logger.warning(f'no wiki page {tr_sub.subreddit_name}..will create')
                wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).wiki.create(
                    MAIN_BOT_NAME, DEFAULT_CONFIG.replace("subredditname", tr_sub.subreddit_name),
                    reason="default_config"
                )

                wd.ri.send_modmail(wd, subject=f"[Notification] Config created",
                                    body=f"There was no configuration created for {BOT_NAME} so "
                                         "one was automatically generated. Please check it to make sure it is "
                                         f"what you want. https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{BOT_NAME}")
                tr_sub.active_status = SubStatus.ACTIVE.value
                wd.s.add(tr_sub)
        except prawcore.exceptions.NotFound:
            logger.warning(f'no config accessible for {tr_sub.subreddit_name}')
            tr_sub.active_status = SubStatus.CONFIG_ACCESS_ERROR.value
            wd.s.add(tr_sub)
    else:
        message.reply(body=f"Invitation received. Please wait for approval by bot owner. In the mean time, "
                           f"you may create a config at https://www.reddit.com/r/{subreddit_name}/wiki/{BOT_NAME} .")
    message.mark_read()


def handle_modmail_message(wd: WorkingData, convo):
    # Ignore old messages past 24h
    if iso8601.parse_date(convo.last_updated) < datetime.now(timezone.utc) - timedelta(hours=24):
        convo.read()

        return
    initiating_author_name = convo.authors[0].name  # praw query
    subreddit_name = convo.owner.display_name  # praw query
    tr_sub = get_subreddit_by_name(wd, subreddit_name=subreddit_name, create_if_not_exist=True, update_if_due=True)
    if not tr_sub:
        return

    # print(f"catching convoid {convo.id} {initiating_author_name}")
    if not tr_sub.mm_convo_id and initiating_author_name and initiating_author_name.lower() == BOT_NAME.lower():
        tr_sub.mm_convo_id = convo.id
        wd.s.add(tr_sub)
        wd.s.commit()


    # Ignore if already actioned (at this many message #s)
    if check_actioned(wd, "mm{}-{}".format(convo.id, convo.num_messages)):  # sql query
        try:
            convo.read()  # praw query
            convo.read()  # praw query24
        except prawcore.exceptions.Forbidden:
            pass

        return

    if initiating_author_name:
        subreddit_author: SubAuthor = wd.s.query(SubAuthor).get((subreddit_name, initiating_author_name))
        if subreddit_author and subreddit_author.currently_blacklisted:
            convo.reply("This author is modmail-blacklisted", internal=True)
            convo.archive()

            return

    response = None
    response_internal = False
    command = "no_command"
    debug_notify = False
    last_post = None

    # If this conversation only has one message -> canned response or summary table
    # Does not respond if already responded to by a mod
    if convo.num_messages == 1 \
            and initiating_author_name not in tr_sub.subreddit_mods \
            and initiating_author_name not in ("AutoModerator", "Sub_Mentions", "mod_mailer"):
        # Join request
        if "ADD USER" in convo.messages[0].body:
            record_actioned(wd, "mm{}-{}".format(convo.id, convo.num_messages))
            return

        import re
        submission = None
        urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
        if len(urls) == 2:  # both link and link description
            print(f"found url: {urls[0][1]}")
            submission = wd.ri.reddit_client.submission(urls[0][1])

        # Automated approval (used for new account screening - like in /r/dating)
        if tr_sub.modmail_auto_approve_messages_with_links:

            urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
            if submission:
                try:
                    in_submission_urls = re.findall(LINK_REGEX, submission.selftext)
                    bad_words = "Raya", 'raya', 'dating app'
                    if not in_submission_urls and 'http' not in submission.selftext \
                            and submission.banned_by and submission.banned_by == "AutoModerator" \
                            and not any(bad_word in submission.selftext for bad_word in bad_words):
                        submission.mod.approve()
                        response = "Since you contacted the mods this bot " \
                                   "has approved your post on a preliminary basis. " \
                                   " The subreddit moderators may override this decision, however\n\n Your text:\n\n>" \
                                   + submission.selftext.replace("\n\n", "\n\n>")
                except prawcore.exceptions.NotFound:
                    pass

        # Non-ALL canned reply - more personalized reply
        if not response:
            # first check if any posts exist for person
            recent_posts: List[SubmittedPost] = wd.s.query(SubmittedPost) \
                .filter(SubmittedPost.subreddit_name.ilike(subreddit_name)) \
                .filter(SubmittedPost.author == initiating_author_name).all()
            removal_reason = None
            # Check again if still no posts in database
            if not recent_posts:
                check_spam_submissions(wd, sub_list=subreddit_name)
                recent_posts: List[SubmittedPost] = wd.s.query(SubmittedPost) \
                    .filter(SubmittedPost.subreddit_name.ilike(subreddit_name)) \
                    .filter(SubmittedPost.author == initiating_author_name).all()
            # Collect removal reason if possible from bot comment or mod comment
            last_post = None
            if submission:
                last_post = wd.s.query(SubmittedPost).get(submission.id)
                if last_post:
                    last_post.api_handle = submission
            if recent_posts:
                last_post = recent_posts[-1] if not last_post else last_post
                # if removal reason hasn't been pulled, try pulling again
                if not last_post.bot_comment_id:
                    posted_status = wd.ri.get_posted_status(last_post, get_removed_info=True)
                    # update db if found an explanation
                    if last_post.bot_comment_id:  # found it now?
                        wd.s.add(last_post)
                        wd.s.commit()
                        removal_reason = wd.ri.get_removed_explanation(last_post)  # try to get removal reason
                        if removal_reason:
                            removal_reason = removal_reason.replace("\n\n", "\n\n>")
                            removal_reason = f"-------------------------------------------------\n\n{removal_reason}"
                    if not removal_reason:  # still couldn't find removal reason, just use posted status
                        removal_reason = f"status: {posted_status.value}\n\n " \
                                         f"flair: {wd.ri.get_submission_api_handle(last_post).link_flair_text}"
                        if posted_status == PostedStatus.SPAM_FLT:
                            removal_reason += " \n\nThis means the Reddit spam filter thought your post was spam " \
                                              "and it was NOT removed by the subreddit moderators.  You can try " \
                                              "verifying your email and building up karma to avoid the spam filter." \
                                              "There is more information here: " \
                                              "https://www.reddit.com/r/NewToReddit/wiki/ntr-guidetoreddit"
                        r_last_post = wd.ri.get_submission_api_handle(last_post)
                        if hasattr(r_last_post, 'removal_reason') and r_last_post.removal_reason \
                                and not posted_status == posted_status.UP:
                            removal_reason += f"\n\n{r_last_post.removal_reason}"

            # All reply if specified
            if not response and tr_sub.modmail_all_reply and tr_sub.modmail_all_reply is not True:
                # response = populate_tags(tr_sub.modmail_all_reply, None, tr_sub=tr_sub, prev_posts=recent_posts)
                response = tr_sub.populate_tags2(tr_sub.modmail_all_reply, post_list=recent_posts)
            # No links auto reply if specified
            if not response and tr_sub.modmail_no_link_reply:
                import re
                urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
                if len(urls) < 2:  # both link and link description
                    # response = populate_tags(tr_sub.modmail_no_link_reply, None, tr_sub=tr_sub,
                    # prev_posts=recent_posts)
                    response = tr_sub.populate_tags2(tr_sub.modmail_no_link_reply, post_list=recent_posts)
                    # Add last found link
                    if recent_posts:
                        response += f"\n\nAre you by chance referring to this post? " \
                                    f"{recent_posts[-1].get_comments_url()}"
                        # Add removal reason if found
                        if removal_reason:
                            response += f"\n\nIf so, does this answer your question?\n\n>{removal_reason}"
                    # debug_notify = True

            # Having previous posts reply
            if not response and recent_posts and tr_sub.modmail_posts_reply:
                # Does have recent posts -> reply with posts reply
                if tr_sub.modmail_posts_reply is True:  # default -> only goes to internal
                    response = ">" + convo.messages[0].body_markdown.replace("\n\n", "\n\n>")
                    if removal_reason and tr_sub.modmail_removal_reason_helper:
                        # response += "\n\n-------------------------------------------------"
                        # response += f"\n\nRemoval reason from [post]
                        # ({last_post.get_comments_url()}):\n\n {removal_reason})"
                        # response += "\n\n-------------------------------------------------"
                        non_internal_response = f"AUTOMATED RESPONSE with reference information " \
                                                f"(please ignore unless relevant):\n\n " \
                                                f"last post: [{last_post.title}]({last_post.get_comments_url()})\n\n" \
                                                f"{removal_reason}"
                        if "status:mod-removed" not in non_internal_response \
                                and "status: AutoMod-removed" not in non_internal_response:
                            # don't answer if not particularly helpful
                            convo.reply(non_internal_response, internal=False)
                    smart_link = f"https://old.reddit.com/message/compose?to={BOT_NAME}" \
                                 f"&subject={subreddit_name}:{convo.id}" \
                                 f"&message="
                    response += tr_sub.populate_tags2(
                        "\n\n{summary table}\n\n"
                        f"Available 'smart links': | "
                        f"[$update]({smart_link}$update) | "
                        f"[$summary {initiating_author_name}]({smart_link}$summary {initiating_author_name}) | "
                        f"[$hallpass {initiating_author_name}]({smart_link}$hallpass {initiating_author_name}) | "
                        # f"[$ban {initiating_author_name}]({smart_link}$ban {initiating_author_name} length reason) | "   
                        # need to fix parameters, length reason
                        f"[$unban {initiating_author_name}]({smart_link}$unban {initiating_author_name}) | "
                        f"[$approve {last_post.id}]({smart_link}$approve {last_post.id}) | "
                        f"[$remove {last_post.id}]({smart_link}$remove {last_post.id}) | "
                        f"\n\nDO NOT CLICK ON LinkedIn LINKS OR URL shorteners - they have been used to dox moderators."
                        f"\n\nPlease subscribe to /r/ModeratelyHelpfulBot for updates.\n\n", None,
                        post_list=recent_posts)

                    response_internal = True
                # Reply using a specified template
                else:  # given a response to say -> not internal
                    # response = populate_tags(tr_sub.modmail_posts_reply, None, prev_posts=recent_posts)
                    response = tr_sub.populate_tags2(tr_sub.modmail_posts_reply, post_list=recent_posts)

            # No posts reply
            elif not response and not recent_posts and tr_sub.modmail_no_posts_reply:
                response = ">" + convo.messages[0].body_markdown.replace("\n\n", "\n\n>") + "\n\n\n\n"
                # response += populate_tags(tr_sub.modmail_no_posts_reply, None, tr_sub=tr_sub)
                response += tr_sub.populate_tags2(tr_sub.modmail_no_posts_reply)
                response_internal = tr_sub.modmail_no_posts_reply_internal
            # debug_notify = True
    else:
        # look for commands if message count >1 and message by mod
        last_author_name: str = convo.messages[-1].author.name
        last_message = convo.messages[-1]
        body_parts: List[str] = last_message.body_markdown.split(' ')
        command: str = body_parts[0].lower() if len(body_parts) > 0 else None
        if last_author_name != BOT_NAME and last_author_name in tr_sub.subreddit_mods:
            # check if forgot to reply as the subreddit
            if command.startswith("$") or command in ('summary', 'update'):
                response, response_internal = handle_dm_command(wd, subreddit_name, last_author_name, command,
                                                                body_parts[1:])
            # Catch messages that weren't meant to be internal
            """  doesn't work anymore?
            elif convo.num_messages > 2 and convo.messages[-2].author.name == BOT_NAME and last_message.is_internal:
                if not check_actioned(f"ic-{convo.id}") and tr_sub.modmail_notify_replied_internal:
                    response = "Hey sorry to bug you, but was this last message not meant to be moderator-only?  " \
                               f"https://mod.reddit.com/mail/perma/{convo.id} \n\n" \
                               "Set `modmail_notify_replied_internal: false` to disable this message"

                    response_internal = True
                    record_actioned(wd, f"ic-{convo.id}")
                    tr_sub.send_modmail(
                        subject="[Notification] Possible moderator-only reply not meant to be moderator-only",
                        body=response)
                    response = None
            """
    if response:
        try:
            convo.reply(body=tr_sub.populate_tags2(response[0:9999], recent_post=last_post), internal=response_internal)

            bot_owner_message = f"subreddit: {subreddit_name}\n\nresponse:\n\n{response}\n\n" \
                                f"https://mod.reddit.com/mail/all/{convo.id}"[0:9999]

            if debug_notify:
                # wd.ri.reddit_client.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
                wd.ri.send_modmail(subreddit_name=BOT_NAME, subject="[Notification] MHB Command used",
                                   body=bot_owner_message, use_same_thread=True)
        except (prawcore.exceptions.BadRequest, praw.exceptions.RedditAPIException):
            logger.debug("reply failed {0}".format(response))
    record_actioned(wd, f"mm{convo.id}-{convo.num_messages}")
    convo.read()
    wd.s.commit()


def handle_modmail_messages(wd: WorkingData):
    print("checking modmail")

    for convo in wd.ri.reddit_client.subreddit('all').modmail.conversations(state="mod", sort='unread', limit=15):
        handle_modmail_message(wd, convo=convo)

    for convo in wd.ri.reddit_client.subreddit('all').modmail.conversations(state="join_requests", sort='unread',
                                                                            limit=15):
        handle_modmail_message(wd, convo=convo)

    for convo in wd.ri.reddit_client.subreddit('all').modmail.conversations(state="all", sort='unread', limit=15):
        handle_modmail_message(wd, convo=convo)

    # properties for message: body_markdown, author.name, id, is_internal, date
    # properties for convo: authors (list), messages,
    # mod_actions, num_messages, obj_ids, owner (subreddit obj), state, subject, user


main_loop()
