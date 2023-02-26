""" None of these are implemented yet """


def load_new_submissions_db():  #requires database:  memory -> db
    global NEW_SUBMISSION_Q
    global SPAM_SUBMISSION_Q
    global SUBREDDIT_Q
    subreddit_names = []
    subreddit_names_complete = []
    for j in [NEW_SUBMISSION_Q,SPAM_SUBMISSION_Q]:
        assert instanceof(j, Queue.queue)
        submission_list = j.get()
        count = 0
        logger.info(f"putting new posts into database")

        for post_to_review in submission_list:
            subreddit_name = str(post_to_review.subreddit).lower()
            if intensity==0 and subreddit_name in subreddit_names_complete:
                continue
            previous_post: SubmittedPost = s.query(SubmittedPost).get(post_to_review.id)
            if previous_post:  # Already saw this post
                if j==NEW_SUBMISSION_Q:
                    subreddit_names_complete.append(subreddit_name)
                continue
            post = SubmittedPost(post_to_review)
            if subreddit_name not in subreddit_names:
                subreddit_names.append(subreddit_name)
            s.add(post)
            if j==SPAM_SUBMISSION_Q:
                subreddit_author: SubAuthor = s.query(SubAuthor).get((sub_list, post.author))
                if subreddit_author and subreddit_author.hall_pass >= 1:
                    subreddit_author.hall_pass -= 1
                    post.api_handle.mod.approve()    # REDDITAPI
                    logger.info('hall pass approval')
                    s.add(subreddit_author)
            count += 1
        j.task_done()
    SUBREDDIT_Q.put(subreddit_names)


def look_for_rule_violations_db(intensity = 0):  # requires db -> memory
    global REDDIT_CLIENT
    global SUBREDDIT_Q
    global POSTINGS_Q


    logger.debug("finding possible duplicates")
    faster_statement = "select max(t.id), group_concat(t.id order by t.id), group_concat(t.reviewed order by t.id), t.author, t.subreddit_name, count(t.author), max( t.time_utc), t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60, s.active_status from RedditPost t inner join TrackedSubs s on t.subreddit_name = s.subreddit_name where s.active_status >3 and counted_status <2 and t.time_utc> utc_timestamp() - Interval s.min_post_interval_mins  minute and t.time_utc > utc_timestamp() - Interval 48 hour group by t.author, t.subreddit_name having count(t.author) > s.max_count_per_interval and (max(t.time_utc)> max(t.last_checked) or max(t.last_checked) is NULL) order by max(t.time_utc) desc ;"
    more_accurate_statement = "SELECT MAX(t.id), GROUP_CONCAT(t.id ORDER BY t.id), GROUP_CONCAT(t.reviewed ORDER BY t.id), t.author, t.subreddit_name, COUNT(t.author), MAX(t.time_utc) as most_recent, t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60, s.active_status FROM RedditPost t INNER JOIN TrackedSubs s ON t.subreddit_name = s.subreddit_name WHERE s.active_status >3 and counted_status <2 AND t.time_utc > utc_timestamp() - INTERVAL s.min_post_interval_mins MINUTE  GROUP BY t.author, t.subreddit_name HAVING COUNT(t.author) > s.max_count_per_interval AND most_recent > utc_timestamp() - INTERVAL 72 HOUR AND (most_recent > MAX(t.last_checked) or max(t.last_checked) is NULL) ORDER BY most_recent desc ;"

    if POSTINGS_Q.size()>0:
        subs_to_update = POSTINGS_Q.get()
        POSTINGS_Q.task_done()
    if subs_to_update and intensity <5:
        sub_list = str(subs_to_update).replace("[", "(").replace("]", ")")
        faster_statement = f"select max(t.id), group_concat(t.id order by t.id), group_concat(t.reviewed order by t.id), t.author, t.subreddit_name, count(t.author), max( t.time_utc), t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60, s.active_status from RedditPost t inner join TrackedSubs s on t.subreddit_name = s.subreddit_name where s.subreddit_name IN {sub_list} and s.active_status >3 and counted_status <2 and t.time_utc> utc_timestamp() - Interval s.min_post_interval_mins  minute and t.time_utc > utc_timestamp() - Interval 48 hour group by t.author, t.subreddit_name having count(t.author) > s.max_count_per_interval and (max(t.time_utc)> max(t.last_checked) or max(t.last_checked) is NULL) order by max(t.time_utc) desc ;"
        #faster_statement = f"SELECT MAX(t.id), GROUP_CONCAT(t.id ORDER BY t.id), GROUP_CONCAT(t.reviewed ORDER BY t.id), t.author, t.subreddit_name, COUNT(t.author), MAX(t.time_utc) as most_recent, t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60, s.active_status FROM RedditPost t INNER JOIN TrackedSubs s ON t.subreddit_name = s.subreddit_name WHERE s.subreddit_name in {sub_list} and s.active_status >3 and counted_status <2 AND t.time_utc > utc_timestamp() - INTERVAL s.min_post_interval_mins MINUTE  GROUP BY t.author, t.subreddit_name HAVING COUNT(t.author) > s.max_count_per_interval AND most_recent > utc_timestamp() - INTERVAL 48 HOUR AND (most_recent > MAX(t.last_checked) or max(t.last_checked) is NULL) ORDER BY most_recent desc ;"

    search_back=48
    if len(posting_groups)  <10:
        search_back=72
    if len(posting_groups) > 150:
        search_back=24
    faster_statement = faster_statement.replace('48', str(search_back))
    more_accurate_statement = more_accurate_statement.replace('48', str(search_back))
    tick = datetime.now()
    if intensity == 5:
        print("doing more accurate")
        rs = s.execute(more_accurate_statement)
    else:
        print("doing usual")
        rs = s.execute(faster_statement)
    print(f"query took this long {datetime.now() - tick}")


    #posting_groups=[]
    for row in rs:
        print(row[0], row[1], row[2], row[3], row[4])
        post_ids = row[1].split(',')
        posts = []
        for post_id in post_ids:
            # print(f"\t{post_id}")
            posts.append(s.query(SubmittedPost).get(post_id))
        last_post = posts[-1]
        assert isinstance(last_post,SubmittedPost)
        if not last_post.review_debug:
            posting_groups.append(PostingGroup(last_post.id, author_name=row[3], subreddit_name=row[4].lower(), posts=posts))
            last_post.review_debug = row[1]
            s.add(last_post)
        else:
            print(f"skipped {last_post.id}--already need to check")
    s.commit()

    print(f"Total found: {len(posting_groups)}")
    tick = datetime.now(pytz.utc)


def review_posts():   #requires mem, reddit?
    posting_groups.sort(key=lambda y: y.posts[-1].id, reverse=True)
    # Go through posting group
    for i, pg in enumerate(posting_groups):
        print(f"========================{i+1}/{len(posting_groups)}============{search_back}=====================")

        # Break if taking too long
        tock = datetime.now(pytz.utc) - tick
        if tock > timedelta(minutes=5) and intensity==0:
            logger.debug("Aborting, taking more than 5 min")
            s.commit()
            break

        # Load subreddit settings
        tr_sub = update_list_with_subreddit(pg.subreddit_name, request_update_if_needed=True)
        max_count = tr_sub.max_count_per_interval
        if tr_sub.active_status_enum not in (SubStatus.ACTIVE, SubStatus.NO_BAN_ACCESS):
            continue

        # Check if they're on the soft blacklist
        subreddit_author: SubAuthor = s.query(SubAuthor).get((pg.subreddit_name, pg.author_name))

        # Remove any posts that are prior to eligibility
        left_over_posts = []
        print(f"---max_count: {max_count}, interval:{tr_sub.min_post_interval_txt} "
              f"grace_period:{tr_sub.grace_period}")
        for j, post in enumerate(pg.posts):

            logger.info(f"{i}-{j}Checking: r/{pg.subreddit_name}  {pg.author_name}  {post.time_utc}  {post.reviewed}  {post.counted_status}"
                        f"url:{post.get_url()}  title:{post.title[0:30]}")

            if post.counted_status == CountedStatus.BLKLIST.value:  ## May not need this later
                logger.info(
                    f"{i}-{j}\t\tAlready handled")
                continue
            # Check for soft blacklist
            if subreddit_author and post.time_utc < subreddit_author.next_eligible:

                logger.info(
                    f"{i}-{j}\t\tpost removed - prior to eligibility")
                try:
                    success = post.mod_remove()  # no checking if can't remove post  # REDDITAPI
                    if success and tr_sub.comment:
                        last_valid_post: SubmittedPost = s.query(SubmittedPost).get(
                            subreddit_author.last_valid_post) if subreddit_author.last_valid_post is not None else None
                        make_comment(tr_sub, post, [last_valid_post, ],
                                     tr_sub.comment, distinguish=tr_sub.distinguish, approve=tr_sub.approve,
                                     lock_thread=tr_sub.lock_thread, stickied=tr_sub.comment_stickied,
                                     next_eligibility=subreddit_author.next_eligible, blacklist=True)
                        post.update_status(reviewed=True, flagged_duplicate=True, counted_status=CountedStatus.BLKLIST)
                        s.add(post)
                except (praw.exceptions.APIException, prawcore.exceptions.Forbidden) as e:
                    logger.warning(f'something went wrong in removing post {str(e)}')
            # Check for post exemptions
            if not post.reviewed:

                counted_status, result = check_for_post_exemptions(tr_sub, post)
                post.update_status(counted_status=counted_status)
                s.add(post)
                logger.info(f"\t\tpost status: {counted_status} {result}")
                if counted_status == CountedStatus.COUNTS:
                    left_over_posts.append(post)

            else:
                logger.info(f"{i}-{j}\t\tpost status: "
                            f"already reviewed {post.counted_status} "
                            f"{'---MHB removed' if post.flagged_duplicate else ''}")

        """
        # Skip if we don't need to go through each post
        if len(left_over_posts) < max_count:
            logger.info("Did not collect enough counted posts")
            s.commit()
            continue
        """

        s.commit()

        # Collect all relevant posts
        back_posts = s.query(SubmittedPost) \
            .filter(
                    # SubmittedPost.flagged_duplicate.is_(False), # redundant with new flag
                    SubmittedPost.subreddit_name.ilike(tr_sub.subreddit_name),
                    SubmittedPost.time_utc > pg.posts[0].time_utc - tr_sub.min_post_interval + tr_sub.grace_period,
                    SubmittedPost.time_utc < pg.posts[-1].time_utc,  # posts not after last post in question
                    SubmittedPost.author == pg.author_name,
                    SubmittedPost.counted_status < 3) \
            .order_by(SubmittedPost.time_utc) \
            .all()

        possible_pre_posts = []
        logger.info(f"Found {len(back_posts)} backposts")
        if len(back_posts) == 0:
            if pg.posts[-1].counted_status != CountedStatus.EXEMPTED.value:
                pg.posts[-1].update_status(reviewed=True)
                s.add(pg.posts[-1])

            logger.info("Nothing to do, moving on.")
            continue
        # Look for exempted posts
        for j, post in enumerate(back_posts):
            logger.info(f"{i}-{j} Backpost: r/{pg.subreddit_name}  {pg.author_name}  {post.time_utc}  "
                        f"url:{post.get_url()}  title:{post.title[0:30]}")

            counted_status = post.counted_status
            logger.info(f"\tpost_counted_status status: {post.counted_status} ")
            if post.counted_status == CountedStatus.NOT_CHKD.value \
                    or post.counted_status == CountedStatus.PREV_EXEMPT.value\
                    or post.counted_status == CountedStatus.EXEMPTED.value:  # later remove?
                counted_status, result = check_for_post_exemptions(tr_sub, post)
                post.update_status(counted_status=counted_status)
                s.add(post)
                logger.info(f"\tpost_counted_status updated: {post.counted_status} {CountedStatus(post.counted_status)}")
            if post.counted_status == CountedStatus.COUNTS.value:
                logger.info(f"\t....Including")
                possible_pre_posts.append(post)
            else:
                logger.info(f"\t..exempting ")

        # Go through left over posts
        grace_count = 0
        for j, post in enumerate(left_over_posts):
            logger.info(f"{i}-{j} Reviewing: r/{pg.subreddit_name}  {pg.author_name}  {post.time_utc}  "
                        f"url:{post.get_url()}  title:{post.title[0:30]}")

            if post.reviewed or post.counted_status == CountedStatus.BLKLIST.value:  # shouldn't get here??
                print(f"\tAlready reviewed %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
                continue

            # Go through possible preposts for left over post
            associated_reposts = []
            for x in possible_pre_posts:
                print(f"\tpost time:{post.time_utc} prev:{x.time_utc} "
                      f"furthestback: {post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period}")
                if x.time_utc < post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period:
                    if post.time_utc - x.time_utc > tr_sub.min_post_interval:
                        print("\t\t Post too far back")
                    else:
                        print("\t\t Post too far back - only grace peroid")
                        #post.update(counted_status=CountedStatus.GRACE_PERIOD_EXEMPT)
                        #s.add(post)
                    continue
                if x.id == post.id or x.time_utc > post.time_utc:
                    print("\t\t Same or future post - breaking loop")
                    break
                status = x.get_posted_status(get_removed_info=True)
                print(f"\t\tpost status: {status} gp:{tr_sub.grace_period} diff: {post.time_utc - x.time_utc}")
                if status == PostedStatus.SELF_DEL and post.time_utc - x.time_utc < tr_sub.grace_period:
                    print("\t\t Grace period exempt")
                    grace_count += 1
                    if grace_count < 3:
                        print("\t\t Grace period exempt")
                        post.update_status(counted_status=CountedStatus.GRACE_PERIOD_EXEMPT)
                        s.add(post)
                        continue
                    else:
                        print("\t\t Too many grace exemptions")
                associated_reposts.append(x)


            # not enough posts
            if len(associated_reposts) < tr_sub.max_count_per_interval:
                logger.info(f"\tNot enough previous posts: {len(associated_reposts)}/{max_count}: "
                            f"{','.join([x.id for x in associated_reposts])}")
                post.update_status(reviewed=True)
            # Hall pass eligible
            elif subreddit_author and subreddit_author.hall_pass > 0:
                subreddit_author.hall_pass -= 1
                notification_text = f"Hall pass was used by {subreddit_author.author_name}: http://redd.it/{post.id}"
                #REDDIT_CLIENT.redditor(BOT_OWNER).message(pg.subreddit_name, notification_text)
                BOT_SUB.send_modmail(subject="[Notification]  Hall pass was used", body=notification_text)
                tr_sub.send_modmail(subject="[Notification]  Hall pass was used", body=notification_text)
                post.update_status(reviewed=True, counted_status=CountedStatus.HALLPASS)
                s.add(subreddit_author)
            # Must take action on post
            else:
                do_requested_action_for_valid_reposts(tr_sub, post, associated_reposts)
                post.update_status(reviewed=True, counted_status=CountedStatus.FLAGGED, flagged_duplicate=True)
                s.add(post)
                # Keep preduplicate posts to keep track of later
                for post in associated_reposts:
                    post.pre_duplicate = True
                    s.add(post)
                s.commit()  # just did a lot of work, need to save
                check_for_actionable_violations(tr_sub, post, associated_reposts)
            s.add(post)
        s.commit()

    s.commit()


def gather_new_submissions_rp(query_limit=800, sub_list='mod', intensity=0):  # requires reddit | reddit -> memory
    global REDDIT_CLIENT
    global ACTIVE_SUB_LIST
    global NEW_SUBMISSION_Q
    global SPAM_SUBMISSION_Q
    logger.info(f"pulling new posts!  intensity: {intensity}")
    chunk_size = 50 if intensity == 1 else 300
    chunked_list = [ACTIVE_SUB_LIST[j:j + chunk_size] for j in range(0, len(ACTIVE_SUB_LIST), chunk_size)]
    for sub_list in chunked_list:
        sub_list_str = "+".join(sub_list)
        print(len(sub_list_str), sub_list_str)
        # reddit lock here
        NEW_SUBMISSION_Q.put([a for a in REDDIT_CLIENT.subreddit(sub_list).new(limit=query_limit)])  # REDDITAPI
        SPAM_SUBMISSION_Q.put([a for a in REDDIT_CLIENT.subreddit(sub_list).mod.spam(only='submissions')])   # REDDITAPI
        # reddit release here



def task_loop():
    tasks = s.query(Task).all()
    if len(tasks) ==0:
        pass
        # s.add(Task())
        # s.add(Task())
        # task list:
            # pull new posts
            # pull spam posts
            #
        # s.add(Task("update_sub_list_clean_up", func_name="update_sub_list", run_interval=24*60*60, intensity=1))
        # s.add(Task("update_sub_list", run_interval=10 * 60, intensity=1))
    for task in tasks:
        if not task.last_ran or (task.last_ran + datetime.timedelta(
                seconds=task.run_interval)) < datetime.datetime.now() or task.force_run:
            func_name = task.name
            if func_name in globals():
                globals()[func_name](task)
                task.last_ran = datetime.datetime.now()
                if task.force_run:
                    task.force_run = False
                s.add(task)
                s.commit()

    time.sleep(15)