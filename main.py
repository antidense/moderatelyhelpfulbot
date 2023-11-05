#!/usr/bin/env python3.7
from __future__ import annotations

# from praw import exceptions
# from praw.models import Submission
from static import *
from datetime import datetime, timedelta, timezone
# from typing import List
# import praw
import prawcore
import pytz
from settings import MAIN_BOT_NAME, ACCEPTING_NEW_SUBS, BOT_OWNER
from utils import look_for_rule_violations3
from models.reddit_models import ActionedComments, CommonPost, Stats2, SubAuthor, SubmittedPost, TrackedAuthor, \
    TrackedSubreddit, RedditInterface, Task
# from logger import logger
from core import dbobj
from workingdata import WorkingData
from nsfw_monitoring import check_post_nsfw_eligibility, nsfw_checking
from modmail import handle_modmail_message, handle_modmail_messages, handle_dm_command, handle_direct_messages
from utils import check_spam_submissions, check_new_submissions, do_reddit_actions


from logger import logger as log



if __name__ == '__main__':
    dbobj.load_models()

"""
To do list:
asyncio 
incorporate toolbox? https://www.reddit.com/r/nostalgia/wiki/edit/toolbox check user notes?
active status to an ENUM
add non-binary gender



"""




def check_submissions(wd):
    chunk_size = 150
    assert isinstance(wd.sub_dict, dict)
    wd.sub_list = list(wd.sub_dict.keys())
    chunked_list = [wd.sub_list[j:j + chunk_size] for j in range(0, len(wd.sub_list), chunk_size)]

    for sub_list in chunked_list:
        sub_list_str = "+".join(sub_list)
        check_new_submissions(wd, sub_list=sub_list_str, intensity=0)
        check_spam_submissions(wd, sub_list=sub_list_str, intensity=0)

def main_loop():
    wd: WorkingData = WorkingData()
    wd.s = dbobj.s  # Database Session object
    wd.ri = RedditInterface()  # Reddit API instance
    wd.most_recent_review = None  # not used?
    wd.bot_name = wd.ri.reddit_client.user.me().name  # what is my name?
    log.debug(f"My name is {wd.bot_name}")
    tasks = wd.s.query(Task).all()
    if not tasks:
        tasks_to_populate = [Task(wd, 'purge_old_records', timedelta(hours=12)),
                 Task(wd, 'do_reddit_actions', timedelta(minutes=1)),
                 Task(wd, 'update_sub_list', timedelta(hours=13)),
                 Task(wd, 'handle_direct_messages', timedelta(minutes=1)),
                 Task(wd, 'handle_modmail_messages', timedelta(minutes=1)),
                 Task(wd, 'look_for_rule_violations3', timedelta(minutes=1)),
                 Task(wd, 'check_submissions', timedelta(minutes=1)),
                 Task(wd, 'calculate_stats', timedelta(hours=10)),
                 Task(wd, 'nsfw_checking', timedelta(minutes=20)),
                 ]
        for task in tasks_to_populate:
            wd.s.add(task)
        tasks = wd.s.query(Task).all()
    if False:
        purge_old_records(wd)
        update_sub_list(wd)
        handle_direct_messages(wd)
        handle_modmail_messages(wd)

        # currently disabled:
        # update_common_posts('nostalgia')
        # update_common_posts('homeimprovement')
        # update_TMBR_submissions(look_back=timedelta(days=7))
        #  do_automated_replies()  This is currently disabled!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        # nsfw_checking(wd)
    rate_limiting_errors = 0
    while True:
        for task in tasks:
            return_val = run_task(wd, task)
            if return_val == -1:
                rate_limiting_errors += 1
                if rate_limiting_errors > 2:
                    import time
                    time.sleep(60 * 5)
                    rate_limiting_errors = 0

def run_task(wd:WorkingData, task):


    if task.last_run_dt and task.last_run_dt + timedelta(seconds=task.frequency_secs) > datetime.now():
        log.debug(f"Skipping task as not due for task: {task.target_function}")
        pass
    elif task.error_count > 5 and task.last_run_dt + timedelta(hours=5) > datetime.now():
        # if had multiple erros  and last ran less than five hours ago
        log.debug(f"Skipping task due to previous errors: {task.target_function} {task.last_error}")
    else:

        start_time = datetime.now()
        try:
            log.debug(f"Running task: {task.target_function}, last ran:{task.last_run_dt}")

            globals()[task.target_function](wd)
            end_time = datetime.now()
            task.last_run_dt = start_time
            log.debug(f"Task complete {task.target_function} {end_time - start_time}")
            task.task_durations.append((end_time - start_time).seconds)
        except (prawcore.exceptions.ServerError, prawcore.exceptions.ResponseException):
            wd.s.commit()
            task.error_count += 1
            import traceback
            trace = traceback.format_exc()
            print(trace)
            return -1
        except Exception:
            wd.s.commit()
            import traceback
            trace = traceback.format_exc()
            task.last_error = str(trace)
            print(trace)

def update_sub_list(wd: WorkingData, intensity=0):
    log.info('updating subs..')
    wd.nsfw_monitoring_subs = {}

    trs = wd.s.query(TrackedSubreddit)\
        .filter(~TrackedSubreddit.active_status_enum.in_((SubStatus.SUB_FORBIDDEN, SubStatus.SUB_GONE))).all()

    # go through all subs in database
    for tr in trs:
        assert isinstance(tr, TrackedSubreddit)

        # See if due for complete re-pull from subreddit wiki (do periodically)
        if not tr.config_last_checked\
                or (tr.config_last_checked < datetime.now() - timedelta(days=1))\
                or (not tr.mod_list)\
                or (intensity == 3):
            log.debug(f'***** rechecking...{tr.subreddit_name}, {tr.active_status_enum}'
                  f' last updated:{tr.last_updated} last config check:{tr.config_last_checked}')

            sub_info = wd.ri.get_subreddit_info(tr.subreddit_name)
            tr.update_from_subinfo(sub_info)  # repopulate db with new values/settings from sub
            tr.config_last_checked = datetime.now()  # record this is updated
            wd.s.add(tr)
        if wd.ri.bot_name.lower() == "moderatelyhelpfulbot" and "moderatelyusefulbot" in tr.mod_list.lower():
            tr.active_status_enum = SubStatus.BOT_NOT_PRIMARY
            wd.s.add(tr)

        # skip adding  if config is NOT okay
        if tr.active_status_enum in (SubStatus.YAML_SYNTAX_ERROR, SubStatus.NO_CONFIG, SubStatus.CONFIG_ACCESS_ERROR, SubStatus.BOT_NOT_PRIMARY):
            log.info(f" active status for {tr.subreddit_name} is {tr.active_status_enum},  skipping")
            continue  # don't bother with this

        # Attempt to load config assuming it's okay
        if tr.subreddit_name not in wd.sub_dict:
            worked, status = tr.reload_yaml_settings()
            wd.s.add(tr)
            if not worked:
                log.info(f" active status for {tr.subreddit_name} is {tr.active_status_enum},  skipping")
                continue

        # Add sub to dict to check
        wd.sub_dict[tr.subreddit_name] = tr

        # Add nsfw moderation if applicable:
        if tr.nsfw_pct_moderation:
            wd.nsfw_monitoring_subs[tr.subreddit_name] = tr

        wd.s.commit()
    return


def update_sub_list_force_all(wd: WorkingData):
    update_sub_list(wd, intensity=3)



def purge_old_records(wd: WorkingData):  # requires db only
    purge_statement = "delete t  from RedditPost t inner join TrackedSubs s on t.subreddit_name = s.subreddit_name where  t.time_utc  < utc_timestamp() - INTERVAL greatest(s.min_post_interval_mins, 60*24*10) MINUTE  and t.flagged_duplicate=0 and t.pre_duplicate=0"
    _ = wd.s.execute(purge_statement)


def calculate_stats(wd: WorkingData):
    # Todo: repeat offenders?

    """CREATE TABLE IF NOT EXISTS stats5 (
      subreddit_name VARCHAR(255),
      counted_status VARCHAR(255),
      post_date DATE,
      post_count INT,
      PRIMARY KEY (subreddit_name, counted_status, post_date)
    );
    """

    save_stats_statement = """
    INSERT INTO stats5 (subreddit_name, counted_status, post_date, post_count)
    SELECT rp.subreddit_name, rp.counted_status_enum, DATE(rp.time_utc), COUNT(*)
    FROM RedditPost rp
    INNER JOIN TrackedSubs s
    WHERE rp.time_utc < (utc_timestamp() - INTERVAL 2 DAY) AND rp.time_utc > DATE(utc_timestamp() - INTERVAL 14 DAY)
    GROUP BY rp.subreddit_name, rp.counted_status_enum, DATE(rp.time_utc)
    ON DUPLICATE KEY UPDATE post_count =  IF(post_count < VALUES(post_count), VALUES(post_count), post_count);
    """

    _ = wd.s.execute(save_stats_statement)

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

    # SELECT date, sum(value_int) FROM `Stats2` WHERE stat_name = "flagged_total" group by date;

def update_common_posts(wd: WorkingData, subreddit_name, limit=1000):
    top_posts = [a for a in wd.ri.reddit_client.subreddit(subreddit_name).top(limit=limit)]
    count = 0
    for post_to_review in top_posts:
        previous_post: CommonPost = wd.s.query(CommonPost).get(post_to_review.id)
        if not previous_post:
            post = CommonPost(post_to_review)
            wd.s.add(post)
            count += 1
    log.debug(f'found {count} top posts')
    log.debug("updating database...")
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
        rp.counted_status_enum = CountedStatus.BOT_SPAM
        wd.s.add(rp)

    #for subreddit_name in blurbs:
        #wd.ri.send_modmail(subject=f"[Notification] Post by possible karma hackers:",
        #                   body="".join(blurbs[subreddit_name]), subreddit=subreddit_name, use_same_thread=True)
        # wd.ri.send_modmail(subject=f"[Notification] botspam notification {subreddit_name}",
        #                   body="".join(blurbs[subreddit_name]), subreddit_name=wd.bot_name, use_same_thread=True)

    wd.s.commit()




    # wd.s.commit()

if __name__ == '__main__':
    main_loop()
