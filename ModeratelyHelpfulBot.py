#!/usr/bin/env python3
import humanize
import iso8601
import logging
import pprint
import praw
import prawcore
import pytz
import queue
import threading
import time
import yaml
from datetime import datetime, timedelta, timezone
from praw import exceptions
from praw.models import Subreddit, Submission
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import List

from settings import BOT_NAME, BOT_PW, CLIENT_ID, CLIENT_SECRET, BOT_OWNER, DB_ENGINE

"""
add priority to posts
"""


# Set up database


engine = create_engine(DB_ENGINE)
Base = declarative_base(bind=engine)

# Set up praw
reddit_client = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, password=BOT_PW,
                            user_agent="ModeratelyHelpfulBot v0.4", username=BOT_NAME)

# Set up some global variables
last_checked = datetime.now() - timedelta(days=1)  # type: datetime
response_tail = ""
main_settings = dict()
main_settings['sleep_interval'] = 60
active_submissions = []
watched_subs = dict()


class Broadcast(Base):
    __tablename__ = 'Broadcast'
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    text = Column(String(191), nullable=True)
    subreddit = Column(String(191), nullable=True)
    sent = Column(Boolean, nullable=True)

    def __init__(self, post):
        self.id = post.id

def utcize(dt):
    return dt.replace(tzinfo=timezone.utc)

def utcnow:
    return datetime.now.replace(tzinfo=timezone.utc)

class Author():
    __tablename__ = 'Authors'
    name = Column(String(21), nullable=True, primary_key=True)
    subreddit = Column(String(21), nullable=True)
    whitelist = []

    # last_replied_to

    def __init__(self, author):
        self.name = author.name

### Add BASE!!!(vv)
class SubAuthor(Base):
    __tablename__ = 'SubAuthors'
    subreddit_name = Column(String(21), nullable=False, primary_key=True)
    author_name = Column(String(21), nullable=False, primary_key=True)
    currently_banned = Column(Boolean, default=False )
    ban_count = Column(Boolean, nullable=True, default=0)
    currently_backlisted = Column(Boolean, nullable=True)
    violation_count = Column(Integer, default=0)
    post_ids = Column(UnicodeText, nullable=True)
    last_updated = Column(DateTime, nullable=True, default=datetime.now())
    next_eligible = Column(DateTime, nullable=True, default=datetime(2019, 1, 1, 0, 0))
    ban_last_failed = Column(DateTime, nullable=True)
    #to_keep = Column(Boolean, nullable=True, default=False)

    def __init__(self, subreddit_name: str, author_name: str):
        self.subreddit_name=subreddit_name
        self.author_name=author_name

    def get_post_ids(self):
        return self.post_ids.split(',')

    def append_post_id(self, new_id):
        post_ids =get_post_ids()
        if new_id not in post_ids:
            post_ids.append(new_ids)
            post_ids = sorted(post_ids)
            self.post_ids = ','.join(post_ids)

    def find_previous_posts(self):
        possible_reposts = s.query(SubmittedPost) \
            .filter(SubmittedPost.flagged_duplicate.is_(False)) \
            .filter(SubmittedPost.subreddit.ilike(tr_sub.subreddit_name)) \
            .filter(SubmittedPost.time_utc >
                    recent_post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins) \
            .filter(SubmittedPost.time_utc < recent_post.time_utc) \
            .filter(SubmittedPost.id != recent_post.id) \
            .filter(SubmittedPost.author == recent_post.author) \
            .order_by(SubmittedPost.time_utc) \
            .all()
        most_recent_reposts = []
        for possible_repost in possible_reposts:
            logger.info(
                "possible repost of: {0}... http://redd.it/{1} -{2}".format(possible_repost.title[0:20], possible_repost.id,
                                                                           datetime.now(pytz.utc).replace(
                                                                               tzinfo=timezone.utc) - possible_repost.time_utc.replace(
                                                                               tzinfo=timezone.utc)))
            banned_by = possible_repost.get_api_handle().banned_by
            if tr_sub.ignore_AutoModerator_removed and banned_by == "AutoModerator":
                continue
            if tr_sub.ignore_moderator_removed and banned_by in tr_sub.subreddit_mods:
                continue
            # ignore delete-and-repost (within the grace period)
            if tr_sub.title_exempt_keyword is not None:
                if tr_sub.title_exempt_keyword.lower() in possible_repost.title.lower():
                    continue
            if possible_repost.get_status() is not "up" \
                    and ((recent_post.time_utc.replace(tzinfo=timezone.utc) - possible_repost.time_utc.replace(
                tzinfo=timezone.utc)) < tr_sub.grace_period_mins):
                continue
            most_recent_reposts.append(possible_repost)
            prev_submission = most_recent_reposts[-1]
            next_eligibility = most_recent_reposts[0].time_utc + subreddit.min_post_interval

    def find_actionable_violations(self):
        possible_repost = most_recent_reposts[-1]
        other_spam_by_author = s.query(SubmittedPost) \
            .filter(SubmittedPost.flagged_duplicate.is_(True)) \
            .filter(SubmittedPost.author == recent_post.author) \
            .filter(SubmittedPost.subreddit.ilike(tr_sub.subreddit_name)) \
            .filter(SubmittedPost.time_utc < recent_post.time_utc) \
            .all()

        logger.info("Author {0} had {1} rule violations. Banning if more than {2}"
                    .format(recent_post.author, len(other_spam_by_author), tr_sub.ban_threshold_count))

        if len(other_spam_by_author) >= tr_sub.ban_threshold_count:
            num_days = tr_sub.ban_duration_days
            if num_days < 1:
                num_days = 1
            if num_days > 998:
                num_days = 0

            str_prev_posts = ",".join([" [{0}]({1})".format(a.id, a.get_comments_url()) for a in other_spam_by_author])

            ban_message = "You have made multiple rate-limiting violations (threshold of {0}): {1}.".format(
                tr_sub.ban_threshold_count, str_prev_posts)
            if num_days > 0:
                ban_message += "\n\nYour ban will last {0} days from this message, ending at {1} UTC. " \
                               "**Repeat infractions result in a permanent ban!**" \
                               "".format(num_days, datetime.now(pytz.utc) + timedelta(days=num_days))
            try:
                if num_days > 0:

                    reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                        recent_post.author, ban_note="ModhelpfulBot: repeated spam", ban_message=ban_message[:999],
                        duration=num_days)
                else:

                    reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                        recent_post.author, ban_note="ModhelpfulBot: repeated spam", ban_message=ban_message[:999])
                logger.info("Ban for {0} succeeded".format(recent_post.author))
                response_lines = [
                    "I banned {0} from this sub due to {1} rule violations over the threshold of {2}. "
                    "You can adjust the threshold in your wiki settings.  "
                    "Set 'notify_about_spammers: false' to not receive this message. \n\n.".format(
                        recent_post.author, len(other_spam_by_author),
                        tr_sub.ban_threshold_count)]




    def update(self, post: SubmittedPost):
        self.last_updated = utcnow()
        if utcnow() < utcize(self.next_eligible):
            self.violation_count+=1
            self.to_keep = True
            return "violation"
        else:
            return "no violation"




    # Filter possible reposts (some maybe removed by automoderator or within grace period) - can't do in database
    most_recent_reposts = []
    for possible_repost in possible_reposts:
        logger.info(
            "possible repost of: {0}... http://redd.it/{1} -{2}".format(possible_repost.title[0:20], possible_repost.id,
                                                                       datetime.now(pytz.utc).replace(
                                                                           tzinfo=timezone.utc) - possible_repost.time_utc.replace(
                                                                           tzinfo=timezone.utc)))
        banned_by = possible_repost.get_api_handle().banned_by
        if tr_sub.ignore_AutoModerator_removed and banned_by == "AutoModerator":
            continue
        if tr_sub.ignore_moderator_removed and banned_by in tr_sub.subreddit_mods:
            continue
        # ignore delete-and-repost (within the grace period)
        if tr_sub.title_exempt_keyword is not None:
            if tr_sub.title_exempt_keyword.lower() in possible_repost.title.lower():
                continue
        if possible_repost.get_status() is not "up" \
                and ((recent_post.time_utc.replace(tzinfo=timezone.utc) - possible_repost.time_utc.replace(
            tzinfo=timezone.utc)) < tr_sub.grace_period_mins):
            continue
        most_recent_reposts.append(possible_repost)
    logger.info("----------------total {0} max {1}".format(len(most_recent_reposts), tr_sub.max_count_per_interval))
    return most_recent_reposts

class TrackedSubreddit(Base):
    __tablename__ = 'TrackedSubs7'
    subreddit_name = Column(String(21), nullable=False, primary_key=True)
    checking_mail_enabled = Column(Boolean, nullable=True)
    settings_yaml_txt = Column(UnicodeText, nullable=True)
    settings_yaml = None
    last_updated = Column(DateTime, nullable=True)
    last_error_msg = Column(DateTime, nullable=True)
    save_text = Column(Boolean, nullable=True)
    subreddit_mods = []
    rate_limiting_enabled = False
    min_post_interval_hrs = 72
    min_post_interval = timedelta(hours=72)
    grace_period_mins = timedelta(minutes=30)
    ban_duration_days = 0
    max_count_per_interval = 1
    ignore_AutoModerator_removed = True
    ignore_moderator_removed = True
    ban_threshold_count = 5
    notify_about_spammers = False
    author_exempt_flair_keyword = None
    author_not_exempt_flair_keyword = None
    action = None
    modmail = None
    report_reason = None
    comment = None
    distinguish = True
    exempt_self_posts = False
    exempt_link_posts = False
    exempt_moderator_posts = True
    title_exempt_keyword = None
    modmail_posts_reply = None
    modmail_no_posts_reply = None
    modmail_no_posts_reply_internal = False
    modmail_auto_approve_messages_with_links = False
    modmail_all_reply = None
    approve = False
    lock_thread = True
    comment_stickied = False
    title_not_exempt_keyword = None

    def __init__(self, subreddit_name):
        self.subreddit_name = subreddit_name.lower()
        self.save_text = False
        self.last_updated = datetime(2019, 1, 1, 0, 0)
        self.error_message = datetime(2019, 1, 1, 0, 0)
        self.update_from_yaml(force_update=True)

    def update_from_yaml(self, force_update=False) -> (Boolean, String):
        return_text = "Updated Successfully!"
        subreddit_handle = reddit_client.subreddit(self.subreddit_name)
        self.subreddit_mods = []
        try:
            self.subreddit_mods = list(moderator.name for moderator in subreddit_handle.moderator())
        except prawcore.exceptions.NotFound:
            pass
        if force_update or self.settings_yaml_txt is None:
            try:
                logger.warning('accessing wiki config %s' % self.subreddit_name)
                wiki_page = reddit_client.subreddit(self.subreddit_name).wiki['moderatelyhelpfulbot']
                if wiki_page:
                    self.settings_yaml_txt = wiki_page.content_md
            except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden) as e:
                logger.warning('no config accessible for %s' % self.subreddit_name)
                self.rate_limiting_enabled = False
                return False, str(e)

        if self.settings_yaml_txt is None:
            return False, "Is the wiki updated? I could not find any settings in the wiki"
        try:
            self.settings_yaml = yaml.safe_load(self.settings_yaml_txt)
        except (yaml.scanner.ScannerError, yaml.composer.ComposerError, yaml.parser.ParserError) as e:
            return False, str(e)

        if self.settings_yaml is None:
            return False, "I couldn't get settings from the wiki for some reason :/"

        if 'save_text' in self.settings_yaml:
            self.save_text = self.settings_yaml['save_text']

        if 'post_restriction' in self.settings_yaml:
            pr_settings = self.settings_yaml['post_restriction']
            self.rate_limiting_enabled = True
            possible_settings = (
                'max_count_per_interval',
                'ignore_AutoModerator_removed',
                'ignore_moderator_removed',
                'ban_threshold_count',
                'notify_about_spammers',
                'ban_duration_days',
                'author_exempt_flair_keyword',
                'author_not_exempt_flair_keyword',
                'action',
                'modmail',
                'comment',
                'report_reason',
                'distinguish',
                'exempt_link_posts',
                'exempt_self_posts',
                'title_exempt_keyword',
                'grace_period_mins',
                'min_post_interval_hrs',
                'min_post_interval_mins',
                'approve',
                'lock_thread',
                'comment_stickied',
                'exempt_moderator_posts',
                'title_not_exempt_keyword',

            )
            if not pr_settings:
                return False, "Bad config"
            for pr_setting in pr_settings:
                if pr_setting in possible_settings:
                    setattr(self, pr_setting, pr_settings[pr_setting])
                else:
                    return_text = "Did not understand variable '{}'".format(pr_setting)

            if 'min_post_interval_mins' in pr_settings:
                self.min_post_interval = timedelta(minutes=pr_settings['min_post_interval_mins'])
                self.min_post_interval_hrs = None
            if 'min_post_interval_hrs' in pr_settings:
                self.min_post_interval = timedelta(hours=pr_settings['min_post_interval_hrs'])
                self.min_post_interval_hrs = pr_settings['min_post_interval_hrs']
            if 'grace_period_mins' in pr_settings and pr_settings['grace_period_mins'] is not None:
                self.grace_period_mins = timedelta(minutes=pr_settings['grace_period_mins'])
            if not self.ban_threshold_count:
                self.ban_threshold_count = 5

        if 'modmail' in self.settings_yaml:
            m_settings = self.settings_yaml['modmail']
            possible_settings = ('modmail_no_posts_reply', 'modmail_no_posts_reply_internal', 'modmail_posts_reply',
                                 'modmail_auto_approve_messages_with_links', 'modmail_all_reply',)
            if m_settings:
                for m_setting in m_settings:
                    if m_setting in possible_settings:
                        setattr(self, m_setting, m_settings[m_setting])
                    else:
                        return_text = "Did not understand variable '{}'".format(m_setting)

        if not self.min_post_interval:
            self.min_post_interval = timedelta(hours=72)
        if not self.grace_period_mins:
            self.grace_period_mins = timedelta(minutes=30)

        self.last_updated = datetime.now()
        return True, return_text

    @staticmethod
    def get_subreddit_by_name(subreddit_name: str):
        if subreddit_name.startswith("/r/"):
            subreddit_name = subreddit_name.replace('/r/', '')
        tr_sub = s.query(TrackedSubreddit).get(subreddit_name)
        if not tr_sub:
            try:
                tr_sub = TrackedSubreddit(subreddit_name)
            except prawcore.PrawcoreException:
                return None
        else:
            tr_sub.update_from_yaml(force_update=True)
        return tr_sub

    def get_author_summary(self, author_name: str) -> str:
        recent_posts = s.query(SubmittedPost) \
            .filter(SubmittedPost.subreddit.ilike(self.subreddit_name)) \
            .filter(SubmittedPost.author == author_name) \
            .all()
        if not recent_posts:
            return "No posts found for {0} in {1}".format(author_name, self.subreddit_name)
        response_lines = ["Author report for {0} in {1}\n".format(author_name, self.subreddit_name),
                          '|Time (UTC)|Author|Title|Violation?|\n\n'
                          '|-----|----|----|---|']
        for post in recent_posts:
            rule_violation = "yes" if post.flagged_duplicate else "no"
            response_lines.append(
                "{0}|[{1}](/u/{1})|[{2}]({3})|{4}".format(post.time_utc, post.author, post.title,
                                                          post.get_comments_url(), rule_violation))
        return "\n\n".join(response_lines)

    def get_sub_stats(self) -> str:
        total_reviewed = s.query(SubmittedPost) \
            .filter(SubmittedPost.subreddit.ilike(self.subreddit_name)) \
            .count()
        total_identified = s.query(SubmittedPost) \
            .filter(SubmittedPost.subreddit.ilike(self.subreddit_name)) \
            .filter(SubmittedPost.flagged_duplicate.is_(True)) \
            .count()

        authors = s.query(SubmittedPost, func.count(SubmittedPost.author).label('qty')) \
            .filter(SubmittedPost.subreddit.ilike(self.subreddit_name)) \
            .group_by(SubmittedPost.author).order_by(desc('qty')).limit(10).all().scalar()

        response_lines = ["Stats report for {0} \n\n".format(self.subreddit_name),
                          '|Author|Count|\n\n'
                          '|-----|----|']
        for post, count in authors:
            response_lines.append("|{}|{}|".format(post.author, count))

        return "total_reviewed: {}\n\n" \
               "total_identified: {}" \
               "\n\n{}".format(total_reviewed, total_identified, "\n\n".join(response_lines))


class SubmittedPost(Base):
    __tablename__ = 'RedditPost'
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    author = Column(String(21), nullable=True)
    submission_text = Column(String(191), nullable=True)
    time = Column(DateTime, nullable=False)
    time_utc = Column(DateTime, nullable=False)
    subreddit = Column(String(21), nullable=True)
    #url = Column(String(191), nullable=True)
    banned_by = Column(String(21), nullable=True)
    flagged_duplicate = Column(Boolean, nullable=True)
    pre_duplicate = Column(Boolean, nullable=True)
    self_deleted = Column(Boolean, nullable=True)
    reviewed = Column(Boolean, nullable=True)
    last_checked = Column(DateTime, nullable=False)
    bot_comment_id = Column(String(10), nullable=True)
    api_handle = None
    is_self = Column(Boolean, nullable=True)

    def __init__(self, post: Submission, save_text=False):
        self.id = post.id
        self.title = post.title[0:190]
        self.author = str(post.author)
        if save_text:
            self.submission_text = post.selftext[0:190]
        self.time = datetime.fromtimestamp(post.created)
        self.time_utc = datetime.utcfromtimestamp(post.created_utc)
        self.subreddit = str(post.subreddit).lower()
        self.flagged_duplicate = False
        self.reviewed = False
        self.banned_by = None
        self.api_handle = post
        self.pre_duplicate = False
        self.self_deleted = False
        self.is_self = post.is_self

    def get_url(self) -> str:
        return "http://redd.it/{0}".format(self.id)

    def get_comments_url(self) -> str:
        return "https://www.reddit.com/r/{0}/comments/{1}".format(self.subreddit, self.id)

    def get_api_handle(self):
        if not self.api_handle:
            self.api_handle = reddit_client.submission(id=self.id)
            return self.api_handle
        else:
            return self.api_handle

    def mod_remove(self):
        try:
            self.get_api_handle().mod.remove()
            return True
        except praw.exceptions.APIException:
            logger.warning('something went wrong removing post')
            return False
        except prawcore.exceptions.Forbidden:
            logger.warning('I was not allowed to remove the post')
            return False

    def reply(self, response, distinguish=True, approve=False, lock_thread=True):
        comment = self.get_api_handle().reply(response)
        if lock_thread:
            self.get_api_handle().mod.lock()
        if distinguish:
            comment.mod.distinguish()
        if approve:
            comment.mod.approve()
        return comment

    def get_status(self):
        global BOT_NAME
        self.get_api_handle()
        self.self_deleted = False if self.api_handle.author else True
        self.banned_by = self.api_handle.banned_by
        if not self.banned_by and not self.self_deleted:
            return "up"
        elif self.banned_by is True:
            return "spam filtered"
        elif self.self_deleted:
            return "self-deleted"
        elif self.banned_by == "AutoModerator":
            return "Automod-removed"
        elif self.banned_by == BOT_NAME:
            return "MHB-removed"
        elif "bot" in self.banned_by.lower():
            return "Bot-removed"
        else:
            return "Mod-removed"


class ActionedComments(Base):
    __tablename__ = 'ActionedComments'
    comment_id = Column(String(30), nullable=True, primary_key=True)
    date_actioned = Column(DateTime, nullable=True)

    def __init__(self, comment_id, ):
        self.comment_id = comment_id
        self.date_actioned = datetime.now()


Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
s = Session()
s.rollback()


def already_has_bot_comment(submission):
    global reddit_client
    top_level_comments = list(submission.comments)
    for c in top_level_comments:
        if c.author and c.author.name == BOT_NAME:
            return True
    return False


def find_previous_posts(tr_sub: TrackedSubreddit, recent_post: SubmittedPost):
    # Find other possible reposts by author
    possible_reposts = s.query(SubmittedPost) \
        .filter(SubmittedPost.flagged_duplicate.is_(False)) \
        .filter(SubmittedPost.subreddit.ilike(tr_sub.subreddit_name)) \
        .filter(SubmittedPost.time_utc >
                recent_post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins) \
        .filter(SubmittedPost.time_utc < recent_post.time_utc) \
        .filter(SubmittedPost.id != recent_post.id) \
        .filter(SubmittedPost.author == recent_post.author) \
        .order_by(SubmittedPost.time_utc) \
        .all()

    # Filter possible reposts (some maybe removed by automoderator or within grace period) - can't do in database
    most_recent_reposts = []
    for possible_repost in possible_reposts:
        logger.info(
            "possible repost of: {0}... http://redd.it/{1} -{2}".format(possible_repost.title[0:20], possible_repost.id,
                                                                       datetime.now(pytz.utc).replace(
                                                                           tzinfo=timezone.utc) - possible_repost.time_utc.replace(
                                                                           tzinfo=timezone.utc)))
        banned_by = possible_repost.get_api_handle().banned_by
        if tr_sub.ignore_AutoModerator_removed and banned_by == "AutoModerator":
            continue
        if tr_sub.ignore_moderator_removed and banned_by in tr_sub.subreddit_mods:
            continue
        # ignore delete-and-repost (within the grace period)
        if tr_sub.title_exempt_keyword is not None:
            if tr_sub.title_exempt_keyword.lower() in possible_repost.title.lower():
                continue
        if possible_repost.get_status() is not "up" \
                and ((recent_post.time_utc.replace(tzinfo=timezone.utc) - possible_repost.time_utc.replace(
            tzinfo=timezone.utc)) < tr_sub.grace_period_mins):
            continue
        most_recent_reposts.append(possible_repost)
    logger.info("----------------total {0} max {1}".format(len(most_recent_reposts), tr_sub.max_count_per_interval))
    return most_recent_reposts


def look_for_rule_violations():
    global reddit_client
    global watched_subs
    authors = dict()
    logger.debug("gathering recent post(s)")
    recent_posts = s.query(SubmittedPost) \
        .filter(SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(hours=10)) \
        .filter(SubmittedPost.flagged_duplicate.is_(False)) \
        .filter(SubmittedPost.reviewed.is_(False)) \
        .filter(SubmittedPost.banned_by.is_(None)) \
        .order_by(desc(SubmittedPost.time_utc)) \
        .limit(100).all()
    for index, recent_post in enumerate(recent_posts):
        subreddit_name = recent_post.subreddit.lower()
        if subreddit_name in watched_subs:
            tr_sub = watched_subs[subreddit_name]
        else:
            tr_sub = s.query(TrackedSubreddit).get(subreddit_name)
            if not tr_sub:
                tr_sub = TrackedSubreddit(subreddit_name)
            if not tr_sub:
                continue
            if tr_sub.last_updated < datetime.now() - timedelta(hours=48):
                tr_sub.update_from_yaml(force_update=True)
                purge_old_records_by_subreddit(tr_sub)
            watched_subs[subreddit_name] = tr_sub
            s.add(tr_sub)
            s.commit()

        logger.info("{4}-Checking subm '{0}...' by '{1}' http://redd.it/{2} flair:({3})".format(
            recent_post.title[0:20], recent_post.author, recent_post.id,
            recent_post.get_api_handle().author_flair_text, index))
        logger.info("----------------post time {0} | interval {1}  after {2} sub:{3}".format(
            datetime.now(pytz.utc).replace(tzinfo=timezone.utc) - recent_post.time_utc.replace(tzinfo=timezone.utc),
            tr_sub.min_post_interval,
            recent_post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins,
            recent_post.subreddit, recent_post.time_utc))
        if subreddit_name not in authors:
            authors_tuple = s.query(SubmittedPost.author, func.count(SubmittedPost.author).label('qty')) \
                .filter(SubmittedPost.subreddit.ilike(subreddit_name)) \
                .filter(
                SubmittedPost.time_utc > datetime.now(pytz.utc) - tr_sub.min_post_interval + tr_sub.grace_period_mins) \
                .group_by(SubmittedPost.author).order_by(desc('qty')).all()
            authors[subreddit_name] = dict((x, y) for x, y in authors_tuple)

        # Shortcut - ignore authors in the alst time period
        # careful though!! it's from the most recent post not the actual post time!
        if subreddit_name in authors and recent_post.author in authors[subreddit_name] \
                and recent_post.time_utc.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc).replace(
            tzinfo=timezone.utc) - timedelta(minutes=30):
            author_count = authors[subreddit_name][recent_post.author]

            if author_count <= tr_sub.max_count_per_interval:
                recent_post.reviewed = True
                logger.info("{4}-[{3}] skipping, not enough posts to consider this author {0} {1} max: {2} "
                            .format(recent_post.author, author_count,
                                    tr_sub.max_count_per_interval, subreddit_name, index))
                s.add(recent_post)
                continue

        # check if flair-exempt
        author_flair = recent_post.get_api_handle().author_flair_text
        if tr_sub.author_exempt_flair_keyword and author_flair and tr_sub.author_exempt_flair_keyword in author_flair:
            recent_post.reviewed = True
            logger.info("{0}-[{1}] skipping,flair exempt ".format(index, subreddit_name))
            s.add(recent_post)
            continue

        if author_flair and recent_post.get_api_handle().author_flair_css_class:
            author_flair = recent_post.get_api_handle().author_flair_text + \
                           recent_post.get_api_handle().author_flair_css_class

        if tr_sub.author_not_exempt_flair_keyword:
            if author_flair and tr_sub.author_not_exempt_flair_keyword not in author_flair:
                logger.info("{0}-[{1}] skipping,flair exempt ".format(index, subreddit_name))
                recent_post.reviewed = True
                s.add(recent_post)
                continue
            if not author_flair:
                logger.info("{0}-[{1}] skipping,flair exempt ".format(index, subreddit_name))
                recent_post.reviewed = True
                s.add(recent_post)
                continue

        # check if keyword exempt:
        if tr_sub.title_exempt_keyword is not None:
            if tr_sub.title_exempt_keyword.lower() in recent_post.title.lower():
                recent_post.reviewed = True
                s.add(recent_post)
                logger.info("{0}-[{1}] keyword exempt ".format(index, subreddit_name))
                continue
        # check if keyword exempt:
        if tr_sub.title_not_exempt_keyword is not None:
            if tr_sub.title_exempt_keyword.lower() not in recent_post.title.lower():
                recent_post.reviewed = True
                s.add(recent_post)
                logger.info("{0}-[{1}] keyword not NOT exempt ".format(index, subreddit_name))
                continue

        # Check if any post type restrictions
        is_self = recent_post.get_api_handle().is_self
        if is_self is True and tr_sub.exempt_self_posts is True:
            logger.info("{0}-[{1}] post type exempt ".format(index, subreddit_name))
            recent_post.reviewed = True
            s.add(recent_post)
            continue
        if is_self is not True and tr_sub.exempt_link_posts is True:
            logger.info("{0}-[{1}] post type exempt ".format(index, subreddit_name))
            recent_post.reviewed = True
            s.add(recent_post)
            continue

        # checking if previously removed
        if recent_post.get_api_handle().banned_by:
            # update post in database
            recent_post.banned_by = recent_post.get_api_handle().banned_by
            author_name = "[deleted]"
            author_r = recent_post.get_api_handle().author
            if author_r:
                author_name = author_r.name
            logger.debug(
                'looks like this is deleted: author={0}, banned_by={1}'.format(author_name, recent_post.banned_by))
            if author_name == "[deleted]":
                recent_post.self_deleted = True
            recent_post.reviewed = True
            s.add(recent_post)
            continue

        # Ignore posts by mods
        if tr_sub.exempt_moderator_posts is True and recent_post.author in tr_sub.subreddit_mods:
            recent_post.reviewed = True
            s.add(recent_post)
            logger.info("{0}-[{1}] mod post exempt ".format(index, subreddit_name))
            continue

        """
        subauthor = SubAuthor.get((subreddit_name, author_name))
        if not subauthor:
        else:
            subauthor = SubAuthor(subreddit_name, author_name)

        subauthor.update(recent_post)
        if to_keep:
            s.add(subauthor)
        """
        associated_reposts = find_previous_posts(tr_sub, recent_post)
        verified_reposts_count = len(associated_reposts)

        # Now check if actually went over threshold
        if verified_reposts_count >= tr_sub.max_count_per_interval:
            do_requested_action_for_valid_reposts(tr_sub, recent_post, associated_reposts)
            recent_post.flagged_duplicate = True
            # Keep preduplicate posts to keep track of later
            for post in associated_reposts:
                post.pre_duplicate = True
                s.add(post)
            check_for_actionable_violations(tr_sub, recent_post, associated_reposts)
        recent_post.reviewed = True
        s.add(recent_post)
    s.commit()


def do_requested_action_for_valid_reposts(tr_sub: TrackedSubreddit, recent_post: SubmittedPost,
                                          most_recent_reposts: List[SubmittedPost]):
    possible_repost = most_recent_reposts[-1]
    if tr_sub.modmail:
        send_modmail(tr_sub, recent_post,
                     possible_repost, tr_sub.modmail)
    if tr_sub.action == "remove":
        was_successful = recent_post.mod_remove()
        if not was_successful:
            return
    if tr_sub.action == "report":
        if tr_sub.report_reason:
            rp_reason = populate_tags(tr_sub.report_reason, recent_post, tr_sub=tr_sub,
                                      prev_post=possible_repost)
            recent_post.get_api_handle().report(("ModeratelyHelpfulBot:" + rp_reason)[0:99])
        else:
            recent_post.get_api_handle().report("ModeratelyHelpfulBot: repeatedly exceeding posting threshold")

    if tr_sub.comment:
        make_comment(tr_sub, recent_post, most_recent_reposts,
                     tr_sub.comment, distinguish=tr_sub.distinguish, approve=tr_sub.approve,
                     lock_thread=tr_sub.lock_thread, stickied=tr_sub.comment_stickied)


def check_for_actionable_violations(tr_sub: TrackedSubreddit, recent_post: SubmittedPost,
                                    most_recent_reposts: List[SubmittedPost]):
    possible_repost = most_recent_reposts[-1]
    other_spam_by_author = s.query(SubmittedPost) \
        .filter(SubmittedPost.flagged_duplicate.is_(True)) \
        .filter(SubmittedPost.author == recent_post.author) \
        .filter(SubmittedPost.subreddit.ilike(tr_sub.subreddit_name)) \
        .filter(SubmittedPost.time_utc < recent_post.time_utc) \
        .all()

    logger.info("Author {0} had {1} rule violations. Banning if more than {2}"
                .format(recent_post.author, len(other_spam_by_author), tr_sub.ban_threshold_count))

    if len(other_spam_by_author) >= tr_sub.ban_threshold_count:
        num_days = tr_sub.ban_duration_days
        if num_days < 1:
            num_days = 1
        if num_days > 998:
            num_days = 0

        str_prev_posts = ",".join([" [{0}]({1})".format(a.id, a.get_comments_url()) for a in other_spam_by_author])

        ban_message = "You have made multiple rate-limiting violations (threshold of {0}): {1}.".format(
            tr_sub.ban_threshold_count, str_prev_posts)
        if num_days > 0:
            ban_message += "\n\nYour ban will last {0} days from this message, ending at {1} UTC. " \
                           "**Repeat infractions result in a permanent ban!**" \
                           "".format(num_days, datetime.now(pytz.utc) + timedelta(days=num_days))
        try:
            if num_days > 0:

                reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                    recent_post.author, ban_note="ModhelpfulBot: repeated spam", ban_message=ban_message[:999],
                    duration=num_days)
            else:

                reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                    recent_post.author, ban_note="ModhelpfulBot: repeated spam", ban_message=ban_message[:999])
            logger.info("Ban for {0} succeeded".format(recent_post.author))
            response_lines = [
                "I banned {0} from this sub due to {1} rule violations over the threshold of {2}. "
                "You can adjust the threshold in your wiki settings.  "
                "Set 'notify_about_spammers: false' to not receive this message. \n\n.".format(
                    recent_post.author, len(other_spam_by_author),
                    tr_sub.ban_threshold_count)]

        except prawcore.exceptions.Forbidden:
            logger.info("Ban for {0} failed".format(recent_post.author))
            response_lines = [
                "This person has multiple rule violations. "
                "Please adjust my privileges and ban threshold "
                "if you would like me to automatically ban them.\n\n".format(
                    recent_post.author, len(other_spam_by_author), tr_sub.ban_threshold_count)]

        for post in other_spam_by_author:
            response_lines.append(
                "* {0}: [{1}](/u/{1}) [{2}]({3})\n".format(post.time_utc, post.author,
                                                           post.title, post.get_comments_url()))
        response_lines.append(
            "* {0}: [{1}](/u/{1}) [{2}]({3})\n".format(recent_post.time_utc, recent_post.author,
                                                       recent_post.title, recent_post.get_comments_url()))
        if tr_sub.notify_about_spammers:
            send_modmail(tr_sub, recent_post,
                         possible_repost, "\n\n".join(response_lines))
    spam_by_similar_authors = s.query(SubmittedPost) \
        .filter(SubmittedPost.title == recent_post.title) \
        .filter(SubmittedPost.author != recent_post.author) \
        .filter(SubmittedPost.subreddit.ilike(tr_sub.subreddit_name)) \
        .all()

    report_lines = []
    if spam_by_similar_authors:
        report_lines.append("---------------WARNING!!!!!!!!!!!!!!!!!!!!!\n\n")
        for i in spam_by_similar_authors[:5]:
            report_lines.append("{}\t{}\t{}\n\n".format(i.author, i.title, i.get_comments_url()))
        for i in other_spam_by_author[:5]:
            report_lines.append("{}\t{}\n\n".format(i.author, i.title, i.get_comments_url()))
        global BOT_OWNER
        # reddit_client.redditor(BOT_OWNER).message("repeat spammer", "".join(report_lines)[:9999])


def populate_tags(input_text, recent_post, tr_sub=None, prev_post=None, prev_posts=None):
    if prev_posts and not prev_post:
        prev_post = prev_posts[0]
    if prev_posts and "{summary table}" in input_text:
        response_lines = ["\n\n|Time|Author|Title|Status|\n"
                          "|:-------|:------|:-----------|:------|\n"]
        for post in prev_posts:
            response_lines.append("|{0}|[{1}](/u/{1})|[{2}]({3})|{4}|\n".format(post.time_utc, post.author, post.title,
                                                                                post.get_comments_url(),
                                                                                post.get_status()))
        final_response = "".join(response_lines)
        input_text = input_text.replace("{summary table}", final_response)

    if prev_post:
        input_text = input_text.replace("{prev.title}", prev_post.title)
        if prev_post.submission_text:
            input_text = input_text.replace("{prev.selftext}", prev_post.submission_text)
        input_text = input_text.replace("{prev.url}", prev_post.get_url())
        input_text = input_text.replace("{time}", prev_post.time_utc.strftime("%Y-%m-%d %H:%M:%S UTC"))
        input_text = input_text.replace("{timedelta}", humanize.naturaltime(datetime.now() - prev_post.time))
    if recent_post:
        input_text = input_text.replace("{author}", recent_post.author)
        input_text = input_text.replace("{title}", recent_post.title)
        input_text = input_text.replace("{url}", recent_post.get_url())

    if tr_sub:
        input_text = input_text.replace("{subreddit}", tr_sub.subreddit_name)
        input_text = input_text.replace("{maxcount}", "{0}".format(tr_sub.max_count_per_interval))
        if tr_sub.min_post_interval_hrs:
            if tr_sub.min_post_interval_hrs < 24:
                input_text = input_text.replace("{interval}", "{0}h".format(tr_sub.min_post_interval_hrs))
            else:
                input_text = input_text.replace(
                    "{interval}", "{0}d{1}h".format(int(tr_sub.min_post_interval_hrs / 24),
                                                    tr_sub.min_post_interval_hrs % 24)).replace("d0h", "d")
        else:
            input_text = input_text.replace("{interval}", "{0}m".format(tr_sub.min_post_interval_mins))
    return input_text


def make_comment(subreddit: TrackedSubreddit, recent_post: SubmittedPost, most_recent_reposts, comment_template: String,
                 distinguish=False, approve=False, lock_thread=True, stickied=False):
    prev_submission = most_recent_reposts[-1]
    next_eligibility = most_recent_reposts[0].time_utc + subreddit.min_post_interval
    ids = " Previous post(s):" \
          + ",".join([" [{0}]({1})".format(a.id, a.get_comments_url()) for a in most_recent_reposts]) \
          + " | limit: {maxcount} per {interval}" \
          + " | next eligiblity: {0}".format(next_eligibility.strftime("%Y-%m-%d %H:%M UTC"))
    ids = ids.replace(" ", " ^^")
    comment = None
    response = populate_tags(comment_template + response_tail + ids,
                             recent_post, tr_sub=subreddit, prev_post=prev_submission)
    try:
        comment = recent_post.reply(response, distinguish=distinguish, approve=approve, lock_thread=lock_thread)
        if stickied:
            comment.mod.distinguish(sticky=True)
    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden) as e:
        logger.warning('something went wrong in creating comment %s', str(e))
    return comment


def send_modmail(subreddit: TrackedSubreddit, recent_post, prev_submission, comment_template):
    response = populate_tags(comment_template, recent_post, tr_sub=subreddit, prev_post=prev_submission)
    try:
        reddit_client.subreddit(subreddit.subreddit_name).message('modhelpfulbot', response)
    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
        logger.warning('something went wrong in sending modmail')


def look_for_similar_titles(subreddit_name):
    recent_posts = s.query(SubmittedPost) \
        .filter(SubmittedPost.subreddit.ilike(subreddit_name)) \
        .filter(SubmittedPost.flagged_duplicate.is_(False)) \
        .filter(SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(days=3))
    for recent_post in recent_posts:
        possible_reposts = s.query(SubmittedPost) \
            .filter(SubmittedPost.flagged_duplicate.is_(False)) \
            .filter(SubmittedPost.ilike(subreddit_name)) \
            .filter(SubmittedPost.id != recent_post.id) \
            .filter(SubmittedPost.time_utc > recent_post.time_utc - timedelta(days=10)) \
            .filter(SubmittedPost.time_utc < recent_post.time_utc) \
            .filter(SubmittedPost.title == recent_post.title) \
            .all()
        # .order_by(desc(func.similarity(SubmittedPost.title, recent_post.title)))\
        # .filter(FullTextSearch(recent_post.title, SubmittedPost.title, FullTextMode.NATURAL))\
        if possible_reposts:
            logger.debug("Checking reposts for '{0}' by '{1}' {2}"
                         .format(recent_post.title, recent_post.author, recent_post.get_url()))
            for possible_repost in possible_reposts:
                logger.debug("\t{0} {1}".format(
                    possible_repost.title, possible_repost.get_url()))
            logger.debug('-------')


def load_settings():
    global main_settings
    global response_tail

    wiki_settings = reddit_client.subreddit('moderatelyhelpfulbot').wiki['moderatelyhelpfulbot']
    main_settings = yaml.safe_load(wiki_settings.content_md)

    if 'response_tail' in main_settings:
        response_tail = main_settings['response_tail']
    # load_subs(main_settings)


def check_actioned(comment_id):
    response = s.query(ActionedComments).get(comment_id)
    if response:
        return True
    return False


def record_actioned(comment_id):
    s.add(ActionedComments(comment_id))
    s.commit()


def send_broadcast_messages():
    global watched_subs
    broadcasts = s.query(Broadcast) \
        .filter(Broadcast.sent.is_(False)) \
        .all()
    if broadcasts:
        update_list_with_all_active_subs()
    try:
        for broadcast in broadcasts:
            if broadcast.subreddit == "all":
                for subreddit_name in watched_subs:
                    reddit_client.subreddit(subreddit_name).message(broadcast.title, broadcast.text)
            else:
                reddit_client.subreddit(broadcast.subreddit).message(broadcast.title, broadcast.text)
            broadcast.sent = True
            s.add(broadcast)

    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
        logger.warning('something went wrong in sending broadcast modmail')
    s.commit()


def do_automated_approvals():
    link_regex = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    reddit_regex = '/r/dating/comments/([^/]*)/'
    subreddits_to_screen = ["dating"]
    for subreddit_name in subreddits_to_screen:

        for convo in reddit_client.subreddit(subreddit_name).modmail.conversations(state="new"):

            # pprint.pprint(vars(convo))
            # pprint.pprint(convo.messages[0].body)
            if check_actioned("screened_" + convo.id) or 'AutoModerator' in convo.authors:
                continue

            for this_message in convo.messages:
                import re
                urls = re.findall(reddit_regex,
                                  this_message.body)
                if len(urls) == 2:  # both link and link description
                    submission = reddit_client.submission(urls[0])

                    in_submission_urls = re.findall(link_regex, submission.selftext)
                    if not in_submission_urls and 'http' not in submission.selftext \
                            and submission.banned_by == "AutoModerator" \
                            and 'dating app' not in (submission.title + submission.selftext):
                        submission.mod.approve()
                        convo.reply(
                            "Since you contacted the mods this bot has approved your post on a preliminary basis. "
                            " The subreddit moderators may override this decision, however")
                        try:
                            convo.mark_read()
                        except AttributeError:
                            logger.warning("Couldn't set as read")
                        break
            record_actioned("screened_" + convo.id)


def handle_direct_messages():
    # Reply to pms or
    global BOT_OWNER
    global watched_subs
    for message in reddit_client.inbox.unread(limit=None):
        logger.info("got this email {0} {1} {2} | {3}".format(message, message.body, message.author, message.subject))

        # Get author name if available.
        author_name = message.author.name if message.author else None

        # Set message_id to root id if part of thread
        message_id = reddit_client.comment(message.id).link_id if message.was_comment else message.name

        # First check if already actioned
        if check_actioned(message_id):
            message.mark_read()  # should have already been "read"
            continue
        # Check if this a user mention (just ignore this)
        elif message.subject.startswith('username mention'):
            message.mark_read()
            continue
        # Check if this is a ban notice (not new modmail)
        elif message.subject.startswith("re: You've been temporarily banned from participating"):
            message.mark_read()
            subreddit_name = message.subject.replace("re: You've been temporarily banned from participating in r/", "")
            if not check_actioned("ban_note: {0}".format(author_name)):
                tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
                if tr_sub and tr_sub.modmail_posts_reply:
                    try:
                        message.reply(tr_sub.get_author_summary(author_name))
                    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                        pass
            record_actioned(check_actioned("ban_note: {0}".format(author_name)))
        elif message.body.lower().startswith("summary"):
            subreddit_name = message.subject.lower().replace("re: ", "")
            tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
            author_name_to_check = message.body.lower().replace("summary ", "")
            if tr_sub:
                message.reply(tr_sub.get_author_summary(author_name_to_check)[:999])
        # Respond to a command (update)
        elif message.body.lower() == "stats":
            subreddit_name = message.subject.lower().replace("re: ", "")
            tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
            if tr_sub:
                message.reply(tr_sub.get_sub_stats())
            message.mark_read()
        elif message.body.lower() == "update":
            subreddit_name = message.subject.lower().replace("re: ", "")
            tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
            if tr_sub:
                moderators = tr_sub.subreddit_mods
                global BOT_OWNER
                if author_name not in moderators and author_name != BOT_OWNER:
                    message.reply(
                        "You do not have permission to do this. Are you sure you are a moderator of {}?".format(
                            subreddit_name))
                else:
                    worked, status = tr_sub.update_from_yaml(force_update=True)
                    reply_text = "Received message to update config for {0}.  See the output below. " \
                                 "If you get a 404 error, it means that the config page needs to be created. " \
                                 "If you get a 503 error, it means the bot doesn't have wiki permissions. " \
                                 "If you get a 'yaml' error, there is an error in your syntax. " \
                                 "Please message [/r/moderatelyhelpfulbot](https://www.reddit.com/" \
                                 "message/compose?to=%2Fr%2Fmoderatelyhelpfulbot) if you have any questions \n\n" \
                                 "Update report: \n\n >{1}" \
                        .format(subreddit_name, status, )
                    message.reply(reply_text)
                    bot_owner_message = "subreddit: {0}\n\nrequestor: {1}\n\nreport: {2}" \
                        .format(subreddit_name, author_name, status)
                    reddit_client.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
                    s.add(tr_sub)
                    s.commit()
            message.mark_read()
            continue
        # Respond to an invitation to moderate
        elif message.subject.startswith('invitation to moderate'):
            subreddit_name = message.subject.replace("invitation to moderate /r/", "")
            sub = reddit_client.subreddit(subreddit_name)
            try:

                sub.mod.accept_invite()
            except praw.exceptions.APIException:
                message.reply("Error: Invite message has been rescinded?")
            message.mark_read()

            message.reply("Hi, thank you for inviting me!  I will start working now. Please make sure I have a config. "
                          "It should be at https://www.reddit.com/r/{0}/wiki/moderatelyhelpfulbot . "
                          "You may need to create it. You can find examples at "
                          "https://www.reddit.com/r/moderatelyhelpfulbot/wiki/index . "
                          .format(subreddit_name))

            # reddit_client.subreddit('moderatelyhelpfulbot').message(subreddit_name, "NOT Added as moderator")
            reddit_client.subreddit('moderatelyhelpfulbot').message(subreddit_name, "Added as moderator")
        # Respond to author (only once)
        elif author_name and not check_actioned(author_name):
            try:
                message.reply("Hi, thank you for messaging me! "
                              "I am only a non-sentient bot so I can't really help you if you have questions. "
                              "Please contact the subreddit moderators. There is a link in my original message :)")
            except prawcore.exceptions.Forbidden:
                pass
            record_actioned(author_name)
        message.mark_read()
        record_actioned(message_id)


def handle_modmail_messages():
    global watched_subs
    subs_to_purge = []
    for convo in reddit_client.subreddit('all').modmail.conversations(state="all", sort='unread', limit=15):
        last_updated_dt = iso8601.parse_date(convo.last_updated)
        if last_updated_dt < datetime.now(timezone.utc) - timedelta(hours=24):
            convo.read()
            continue
        author_name = convo.authors[0].name
        subreddit_name = convo.owner.display_name
        # tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
        if subreddit_name not in watched_subs:
            update_list_with_subreddit(subreddit_name)
        tr_sub = watched_subs[subreddit_name]
        if not tr_sub:
            continue
        subs_to_purge.append(tr_sub)
        if author_name in tr_sub.subreddit_mods:
            convo.read()
            continue
        if author_name == "AutoModerator":
            convo.read()
            continue
        print(
            'test000000000000000000000000000000' + convo.subject + "  " + subreddit_name + " " + str(convo.last_unread))
        if convo.num_messages == 1:
            if check_actioned(convo.id):
                convo.read()
                continue
            if tr_sub.modmail_all_reply:
                response = populate_tags(tr_sub.tr_sub.modmail_all_reply, None, tr_sub=tr_sub)
                convo.read()
                record_actioned(convo.id)
                continue
            recent_posts = s.query(SubmittedPost) \
                .filter(SubmittedPost.subreddit.ilike(subreddit_name)) \
                .filter(SubmittedPost.author == author_name) \
                .all()
            if recent_posts:
                response = populate_tags("{summary table}\n\n**Please don't forget to change to 'reply as the "
                                         "subreddit' below!!**", None, prev_posts=recent_posts)
                try:
                    convo.reply(response, internal=True)
                except prawcore.exceptions.BadRequest:
                    logger.debug("reply failed {0}".format(response))
            else:
                logger.debug("to reply {0}".format(tr_sub.modmail_no_posts_reply))
                if tr_sub.modmail_no_posts_reply:
                    response = populate_tags(tr_sub.modmail_no_posts_reply, None, tr_sub=tr_sub)
                    convo.reply(response, internal=tr_sub.modmail_no_posts_reply_internal)
            convo.read()
            record_actioned(convo.id)
        else:
            print('--------still unread' + str(convo.last_unread))
            last_message = convo.messages[-1]
            if check_actioned(last_message.id):
                convo.read()
                continue
            if last_message.is_internal and "{" in last_message.body_markdown:
                print(last_message.body_markdown)
            convo.read()
            record_actioned(last_message.id)
        convo.read()
    # for sub in subs_to_purge:
    #    if sub:
    #        print("purging "+sub.subreddit_name)
    #        sub.modmail.bulk_read(state='new')

    # properties for message: body_markdown, author.name, id, is_internal, date
    # properties for convo: authors (list), messages,
    # mod_actions, num_messages, obj_ids, owner (subreddit obj), state, subject, user


def most_common(lst):
    return max(set(lst), key=lst.count)


def update_list_with_all_active_subs():
    global watched_subs
    subs = s.query(TrackedSubreddit).filter(TrackedSubreddit.last_updated > datetime.now() - timedelta(days=3)).all()
    for sub in subs:
        if sub.subreddit_name not in watched_subs:
            update_list_with_subreddit(sub.subreddit_name)


def update_list_with_subreddit(subreddit_name: str):
    global watched_subs
    if subreddit_name in ["pokinsfw3"]:
        return None
    tr_sub = s.query(TrackedSubreddit).get(subreddit_name)
    if not tr_sub:
        tr_sub = TrackedSubreddit(subreddit_name)
    else:
        tr_sub.update_from_yaml(force_update=True)

    watched_subs[subreddit_name] = tr_sub
    s.add(tr_sub)
    s.commit()
    return tr_sub


def purge_old_records(days=14):
    to_delete = s.query(SubmittedPost) \
        .filter(SubmittedPost.time_utc < datetime.now() - timedelta(days=days)) \
        .filter(SubmittedPost.flagged_duplicate.is_(False)) \
        .filter(SubmittedPost.pre_duplicate.is_(False)) \
        .delete()
    s.commit()


def purge_old_records_by_subreddit(tr_sub: TrackedSubreddit):
    print("looking for old records to purge from ", tr_sub.subreddit_name, tr_sub.min_post_interval)
    to_delete = s.query(SubmittedPost) \
        .filter(SubmittedPost.time_utc < datetime.now(pytz.utc).replace(tzinfo=None) - tr_sub.min_post_interval) \
        .filter(SubmittedPost.flagged_duplicate.is_(False)) \
        .filter(SubmittedPost.pre_duplicate.is_(False)) \
        .filter(SubmittedPost.subreddit == tr_sub.subreddit_name) \
        .delete()
    # print("purging {} old records from {}", len(to_delete), tr_sub.subreddit_name)
    # to_delete.delete()
    s.commit()


def check_new_submissions2a(query_limit=800):
    global reddit_client
    subreddit_names = []
    subreddit_names_complete = []
    logger.info("pulling new posts!")
    possible_new_posts = [a for a in reddit_client.subreddit('mod').new(limit=query_limit)]
    logger.info('found {0} posts'.format(len(possible_new_posts)))
    for post_to_review in possible_new_posts:
        subreddit_name = str(post_to_review.subreddit).lower()
        if subreddit_name in subreddit_names_complete:
            continue
        previous_post = s.query(SubmittedPost).get(post_to_review.id)
        if previous_post:
            subreddit_names_complete.append(subreddit_name)
            continue
        if not previous_post:
            post = SubmittedPost(post_to_review)
            if subreddit_name not in subreddit_names:
                subreddit_names.append(subreddit_name)
            logger.info("found submitted post: '{0}...' http://redd.it/{1} ({2})".format(post.title[0:20], post.id,
                                                                                         subreddit_name))
            s.add(post)
    logger.debug("updating database...")
    s.commit()
    return subreddit_names


def check_spam_submissions():
    global reddit_client
    possible_spam_posts = [a for a in reddit_client.subreddit('mod').mod.spam(only='submissions')]
    for post_to_review in possible_spam_posts:
        previous_post = s.query(SubmittedPost).get(post_to_review.id)
        if previous_post:
            break
        if not previous_post:
            post = SubmittedPost(post_to_review)
            subreddit_name = post.subreddit.lower()
            logger.info("found spam post: '{0}...' http://redd.it/{1} ({2})".format(post.title[0:20], post.id,
                                                                                    subreddit_name))
            post.reviewed = True
            s.add(post)
    logger.debug("updating database...")
    s.commit()


q = queue.Queue()


def worker():
    for submission in reddit_client.subreddit('mod').stream.submissions():
        q.put(submission)
    time.sleep(5)


def check_new_submissions3():
    for i in range(100):
        item = q.get()
        subreddit_name = str(item.subreddit).lower()
        previous_post = s.query(SubmittedPost).get(item.id)
        if not previous_post:
            post = SubmittedPost(item)
            logger.info("f{3} found submitted post: ' {0}...' http://redd.it/{1} ({2})".format(post.title[0:20],
                                                                                               post.id, subreddit_name,
                                                                                               i))
            s.add(post)
        q.task_done()


def main_loop():
    global watched_subs
    load_settings()
    purge_old_records()
    tr_subs = dict()
    # update_list_with_all_active_subs()
    while True:
        # moderate_debates()
        # scan_comments_for_activity()
        # flag_all_submissions_for_activity()
        # recalculate_active_submissions()
        print('start_loop')
        threading.Thread(target=worker, daemon=True).start()

        # subs_to_update = check_new_submissions2()
        # print("substoupdate:")
        # print(subs_to_update)
        check_spam_submissions()
        check_new_submissions3()

        look_for_rule_violations()

        # update_TMBR_submissions(look_back=timedelta(days=7))
        send_broadcast_messages()
        do_automated_approvals()
        #  do_automated_replies()  This is currently disabled!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        handle_direct_messages()
        handle_modmail_messages()


def get_naughty_list():
    authors_tuple = s.query(SubmittedPost.author, SubmittedPost.subreddit,
                            func.count(SubmittedPost.author).label('qty')) \
        .filter(
        SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(days=30)) \
        .group_by(SubmittedPost.author, SubmittedPost.subreddit).order_by(desc('qty')).limit(80)

    for x, y, z in authors_tuple:
        print("{1}\t{0}\t{2}".format(x, y, z))
    """
    authors_tuple = s.query(SubmittedPost.author, func.count(SubmittedPost.author).label('qty')) \
        .filter(
        SubmittedPost.time_utc > datetime.now(pytz.utc)- timedelta(days=90)) \
        .group_by(SubmittedPost.author).order_by(desc('qty')).limit(40)
    for x, y in authors_tuple:
        print("{1}\t\t{0}".format(x, y))
        """


def init_logger(logger_name, filename=None):
    import os
    if not filename:
        filename = os.path.join(logger_name + '.log')
    global logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter
    # logger.setFormatter(formatter)
    # sh.setFormatter(formatter)
    # add ch to logger
    if len(logger.handlers) == 0:
        # logger.addHandler(file_logger)
        logger.addHandler(sh)
    return logger


# set up the logger
logger = init_logger("mhbot_log")
EASTERN_TZ = pytz.timezone("US/Eastern")

main_loop()
