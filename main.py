#!/usr/bin/env python3.7
from __future__ import annotations

import log

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
    TrackedSubreddit, RedditInterface
# from logger import logger
from core import dbobj
from workingdata import WorkingData
from nsfw_monitoring import check_post_nsfw_eligibility, nsfw_checking
from modmail import handle_modmail_message, handle_modmail_messages, handle_dm_command, handle_direct_messages
from utils import check_spam_submissions, check_new_submissions


FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
#logging.basicConfig(format=FORMAT, level=logging.DEBUG)
log = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
# handler.setFormatter(FORMAT)
log.addHandler(handler)

if __name__ == '__main__':
    dbobj.load_models()

"""
To do list:
asyncio 
incorporate toolbox? https://www.reddit.com/r/nostalgia/wiki/edit/toolbox check user notes?
active status to an ENUM
add non-binary gender



"""


class Task:
    """
    __tablename__ = 'Tasks'
    wd = None
    task_name = Column(String(191), nullable=False, primary_key=True)
    func_name = Column(String(191), nullable=False)
    last_run_dt = Column(DateTime, nullable=True)
    last_runtime = Column(Integer, nullable=False)
    frequency_mins = Column(Integer, nullable=False)
    """

    wd = None
    target_function = None
    last_run_dt = None
    frequency = timedelta(minutes=5)
    max_duration = timedelta(minutes=5)
    task_durations = []
    error_count = 0
    last_error = ""

    def __init__(self, wd, target_function, frequency):
        self.wd = wd
        self.target_function = target_function
        self.frequency = frequency

    def run_task(self):

        if self.last_run_dt and self.last_run_dt + self.frequency > datetime.now():
            log.debug(f"Skipping task as not due for task: {self.target_function}")
            pass
        elif self.error_count > 5:
            log.debug(f"Skipping task due to previous errors: {self.target_function} {self.last_error}")
        else:
            start_time = datetime.now()
            try:
                log.debug(f"Running task: {self.target_function}, last ran:{self.last_run_dt}")
                globals()[self.target_function](self.wd)
                end_time = datetime.now()
                self.last_run_dt = start_time
                log.debug(f"Task complete {self.target_function} {end_time-start_time}")
                self.task_durations.append((end_time-start_time).seconds)
            except (prawcore.exceptions.ServerError, prawcore.exceptions.ResponseException) as e:
                self.error_count += 1
                return -1
            except Exception as e:
                import traceback
                trace = traceback.format_exc()
                self.last_error = str(trace)
                print(trace)
        """
        except (prawcore.exceptions.ServerError, prawcore.exceptions.ResponseException) as e:
            import time
            print("sleeping due to server error")
            import traceback
            print(traceback.format_exc())
            time.sleep(60 * 5)  # sleep for a bit server errors
        
            import traceback
            trace = traceback.format_exc()
            print(trace)
            # wd.ri.send_modmail(subreddit_name=BOT_NAME, subject="[Notification] MHB Exception", body=trace,
            #                    use_same_thread=True)
            #wd.s.add(wd.ri.bot_sub)
            #wd.s.commit()
        """


def check_submissions(wd):
    chunk_size = 300
    assert isinstance(wd.sub_dict, dict)
    wd.sub_list = list(wd.sub_dict.keys())
    print(wd.sub_list)
    chunked_list = [wd.sub_list[j:j + chunk_size] for j in range(0, len(wd.sub_list), chunk_size)]

    for sub_list in chunked_list:
        sub_list_str = "+".join(sub_list)
        print(sub_list_str)
        check_new_submissions(wd, sub_list=sub_list_str, intensity=0)
        check_spam_submissions(wd, sub_list=sub_list_str, intensity=0)

def main_loop():
    wd: WorkingData = WorkingData()
    wd.s = dbobj.s  # Database Session object
    wd.ri = RedditInterface()  # Reddit API instance
    wd.most_recent_review = None  # not used?
    wd.bot_name = wd.ri.reddit_client.user.me().name  # what is my name?
    log.debug(f"My name is {wd.bot_name}")

    tasks = [Task(wd, 'purge_old_records', timedelta(hours=12)),
             Task(wd, 'update_sub_list', timedelta(hours=2)),
             Task(wd, 'handle_direct_messages', timedelta(minutes=1)),
             Task(wd, 'handle_modmail_messages', timedelta(minutes=1)),
             Task(wd, 'look_for_rule_violations3', timedelta(minutes=1)),
             Task(wd, 'check_submissions', timedelta(minutes=1)),
             Task(wd, 'calculate_stats', timedelta(hours=10)),
             Task(wd, 'nsfw_checking', timedelta(minutes=20)),
             ]
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
            return_val = task.run_task()
            if return_val == -1:
                rate_limiting_errors += 1
                if rate_limiting_errors > 2:
                    import time
                    time.sleep(60 * 5)
                    rate_limiting_errors = 0


def update_sub_list(wd: WorkingData, intensity=0):
    print('updating subs..', sep="")
    wd.sub_list = []
    wd.nsfw_monitoring_subs = {}

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

    # go through all subs in database
    for tr in trs:
        assert isinstance(tr, TrackedSubreddit)

        # See if due for complete re-pull from subreddit wiki (do periodically)
        if not tr.config_last_checked\
                or (tr.active_status >= 0 and tr.config_last_checked < datetime.now() - timedelta(days=1))\
                or (tr.active_status >= 0 and not tr.mod_list):
            print(f'...rechecking...{tr.subreddit_name},'
                  f' last updated:{tr.last_updated} last config check:{tr.config_last_checked}')

            sub_info = wd.ri.get_subreddit_info(tr.subreddit_name)
            tr.update_from_subinfo(sub_info)  # repopulate db with new values/settings from sub
            tr.config_last_checked = datetime.now()  # record this is updated
            wd.s.add(tr)

        # skip adding  if config is NOT okay
        if tr.active_status < 4:
            print(f" active status for {tr.subreddit_name} is {tr.active_status},  skipping")
            continue  # don't bother with this

        # Attempt to load config assuming it's okay
        if tr.subreddit_name not in wd.sub_dict:
            worked, status = tr.reload_yaml_settings()
            wd.s.add(tr)
            if not worked:
                print(f" active status for {tr.subreddit_name} is {tr.active_status},  skipping")
                continue

        # Add sub to dict to check
        wd.sub_dict[tr.subreddit_name] = tr

        # Add nsfw moderation if applicable:
        if tr.nsfw_pct_moderation:
            wd.nsfw_monitoring_subs[tr.subreddit_name] = tr

        wd.s.commit()
    return




def purge_old_records(wd: WorkingData):  # requires db only
    purge_statement = "delete t  from RedditPost t inner join TrackedSubs s on t.subreddit_name = s.subreddit_name where  t.time_utc  < utc_timestamp() - INTERVAL greatest(s.min_post_interval_mins, 60*24*10) MINUTE  and t.flagged_duplicate=0 and t.pre_duplicate=0"
    _ = wd.s.execute(purge_statement)


def calculate_stats(wd: WorkingData):
    # Todo: repeat offenders?

    statement = 'select count(*),counted_status, subreddit_name, date(time_utc) as date from RedditPost  where   time_utc < date(utc_timestamp) group by date(time_utc),  subreddit_name,  counted_status order by date desc'
    rs = wd.s.execute(statement)

    for row in rs:
        count = row[0]
        counted_status = row[1]
        subreddit_name = row[2]
        date = row[3]
        stat_name = str(CountedStatus(counted_status)).replace("CountedStatus.", "").lower()[0:21]
        sub_stat = wd.s.query(Stats2).get((subreddit_name, date, stat_name))
        if not sub_stat:
            sub_stat = Stats2(subreddit_name, date, stat_name)
            sub_stat.value_int = count
            wd.s.add(sub_stat)
        elif sub_stat.value_int < count:
            sub_stat.value_int = count
        wd.s.add(sub_stat)
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
        sub_stat = wd.s.query(Stats2).get((subreddit_name, date, 'flagged_'))
        if not sub_stat:
            sub_stat = Stats2(subreddit_name, date, 'flagged_')
            sub_stat.value_int = flagged_count
            wd.s.add(sub_stat)
        sub_stat = wd.s.query(Stats2).get((subreddit_name, date, 'blacklisted_'))
        if not sub_stat:
            sub_stat = Stats2(subreddit_name, date, 'blacklisted_')
            sub_stat.value_int = blacklisted_count
            wd.s.add(sub_stat)
        sub_stat = wd.s.query(Stats2).get((subreddit_name, date, 'removed_'))
        if not sub_stat:
            sub_stat = Stats2(subreddit_name, date, 'removed_')
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
        rp.counted_status = CountedStatus.BOT_SPAM.value
        wd.s.add(rp)

    for subreddit_name in blurbs:
        wd.ri.send_modmail(subject=f"[Notification] Post by possible karma hackers:",
                           body="".join(blurbs[subreddit_name]), subreddit=subreddit_name, use_same_thread=True)
        # wd.ri.send_modmail(subject=f"[Notification] botspam notification {subreddit_name}",
        #                   body="".join(blurbs[subreddit_name]), subreddit_name=wd.bot_name, use_same_thread=True)

    wd.s.commit()




    # wd.s.commit()

if __name__ == '__main__':
    main_loop()
