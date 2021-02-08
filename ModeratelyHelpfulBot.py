#!/usr/bin/env python3
import humanize
import iso8601
import logging
import praw
import prawcore
import pytz
import queue
import time
import yaml
from datetime import datetime, timedelta, timezone
from praw import exceptions
from praw.models import Submission
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import List
from settings import BOT_NAME, BOT_PW, CLIENT_ID, CLIENT_SECRET, BOT_OWNER, DB_ENGINE
import json
"""
To do list:
Did you accidentally forget to "reply to user"?
put back "i couldn't find posts from you" for /r/dating
add priority to posts
asyncio 
check previous spam - for modmail as an option
golden ticket -f reebie without remove
check usernotes?
rate limit modspam
automated approval - put message body
metapost/ post type limit across usernames
"""

ACCEPTING_NEW_SUBS = True
LOOK_BACK_INTERVAL_HRS = 24

# Set up database
engine = create_engine(DB_ENGINE)
Base = declarative_base(bind=engine)

# Set up PRAW
reddit_client = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, password=BOT_PW,
                            user_agent="ModeratelyHelpfulBot v0.4", username=BOT_NAME)

# Set up some global variables
last_checked = datetime.now() - timedelta(days=1)  # type: datetime
response_tail = ""
main_settings = dict()
main_settings['sleep_interval'] = 60
active_submissions = []
watched_subs = dict()
SUBS_WITH_NO_BAN_ACCESS = []


class Broadcast(Base):
    __tablename__ = 'Broadcast'
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    text = Column(String(191), nullable=True)
    subreddit = Column(String(191), nullable=True)
    sent = Column(Boolean, nullable=True)

    def __init__(self, post):
        self.id = post.id


class SubmittedPost(Base):
    __tablename__ = 'RedditPost'
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    author = Column(String(21), nullable=True)
    submission_text = Column(String(191), nullable=True)
    time_utc = Column(DateTime, nullable=False)
    subreddit = Column(String(21), nullable=True)
    banned_by = Column(String(21), nullable=True)
    flagged_duplicate = Column(Boolean, nullable=True)
    pre_duplicate = Column(Boolean, nullable=True)
    self_deleted = Column(Boolean, nullable=True)
    reviewed = Column(Boolean, nullable=True)
    last_checked = Column(DateTime, nullable=False)
    bot_comment_id = Column(String(10), nullable=True)
    is_self = Column(Boolean, nullable=True)
    removed_status = Column(String(21), nullable=True)
    counted_status = Column(SmallInteger, nullable=True, default=-1)  # not-checked=-1,  does_not_count=0, does count=1
    api_handle = None

    def __init__(self, post: Submission, save_text=False):
        self.id = post.id
        self.title = post.title[0:190]
        self.author = str(post.author)
        if save_text:
            self.submission_text = post.selftext[0:190]
        self.time_utc = datetime.utcfromtimestamp(post.created_utc)
        self.subreddit = str(post.subreddit).lower()
        self.flagged_duplicate = False
        self.reviewed = False
        self.banned_by = None
        self.api_handle = post
        self.pre_duplicate = False
        self.self_deleted = False
        self.is_self = post.is_self
        self.counted_status = -1

    def get_url(self) -> str:
        return "http://redd.it/{0}".format(self.id)

    def get_comments_url(self) -> str:
        return "https://www.reddit.com/r/{0}/comments/{1}".format(self.subreddit, self.id)

    def get_api_handle(self) -> praw.models.Submission:
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
            logger.warning('something went wrong removing post: http://redd.it/{0}'.format(self.id))
            return False
        except prawcore.exceptions.Forbidden:
            logger.warning('I was not allowed to remove the post: http://redd.it/{0}'.format(self.id))
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

    def get_status(self, force_update=False):
        # if self.last_status and self.last_checked > datetime.now(pytz.utc)-timedelta(hours=24):
        #    return self.last_status

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


class SubAuthor(Base):
    __tablename__ = 'SubAuthors'
    subreddit_name = Column(String(21), nullable=False, primary_key=True)
    author_name = Column(String(21), nullable=False, primary_key=True)
    currently_banned = Column(Boolean, default=False)
    ban_count = Column(Integer, nullable=True, default=0)
    currently_blacklisted = Column(Boolean, nullable=True)
    violation_count = Column(Integer, default=0)
    post_ids = Column(UnicodeText, nullable=True)
    blacklisted_post_ids = Column(UnicodeText, nullable=True)
    last_updated = Column(DateTime, nullable=True, default=datetime.now())
    next_eligible = Column(DateTime, nullable=True, default=datetime(2019, 1, 1, 0, 0))
    ban_last_failed = Column(DateTime, nullable=True)
    hall_pass = Column(Integer, default=0)

    def __init__(self, subreddit_name: str, author_name: str):
        self.subreddit_name = subreddit_name
        self.author_name = author_name

    def update_post_violation_list(self, post_id, date):

        if not self.post_ids:
            self.post_ids = json.dumps({post_id: date.timestamp()})
        else:
            post_ids_list = json.loads(self.post_ids)
            if post_id not in post_ids_list:
                post_ids_list[post_id] = date.timestamp()
                self.post_ids = json.dumps(post_ids_list)

    def check_if_already_blacklisted(self, post_id):
        if not self.blacklisted_post_ids:
            return False
        else:
            try:
                blacklisted_post_ids_list = json.loads(self.blacklisted_post_ids)
            except json.decoder.JSONDecodeError:
                self.blacklisted_post_ids = None
                blacklisted_post_ids_list = []
                return False
            if post_id in blacklisted_post_ids_list:
                return True
            else:
                return False

    def update_blacklisted_post_list(self, post_id, date):
        if not self.blacklisted_post_ids:
            self.blacklisted_post_ids = json.dumps({post_id: date.timestamp()})
        else:
            try:
                blacklisted_post_ids_list = json.loads(self.blacklisted_post_ids)
            except json.decoder.JSONDecodeError:
                self.blacklisted_post_ids = None
                blacklisted_post_ids_list = []
                return
            if post_id not in blacklisted_post_ids_list:
                blacklisted_post_ids_list[post_id] = date.timestamp()
                self.blacklisted_post_ids = json.dumps(blacklisted_post_ids_list)


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
    title_exempt_keyword = None
    action = None
    modmail = None
    report_reason = None
    comment = None
    distinguish = True
    exempt_self_posts = False
    exempt_link_posts = False
    exempt_moderator_posts = True
    exempt_oc = False
    modmail_posts_reply = None
    modmail_no_posts_reply = None
    modmail_no_posts_reply_internal = False
    modmail_auto_approve_messages_with_links = False
    modmail_all_reply = None
    approve = False
    blacklist_enabled = True
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
            possible_settings = {
                'max_count_per_interval': int,
                'ignore_AutoModerator_removed': bool,
                'ignore_moderator_removed': bool,
                'ban_threshold_count': int,
                'notify_about_spammers': bool,
                'ban_duration_days': int,
                'author_exempt_flair_keyword': str,
                'author_not_exempt_flair_keyword': str,
                'action': str,
                'modmail': str,
                'comment': str,
                'report_reason': str,
                'distinguish': bool,
                'exempt_link_posts': bool,
                'exempt_self_posts': bool,
                'title_exempt_keyword': str,
                'grace_period_mins': int,
                'min_post_interval_hrs': int,
                'min_post_interval_mins': int,
                'approve': bool,
                'lock_thread': bool,
                'comment_stickied': bool,
                'exempt_moderator_posts': bool,
                'exempt_oc': bool,
                'title_not_exempt_keyword': str,
                'blacklist_enabled': bool,

                }
            if not pr_settings:
                return False, "Bad config"
            for pr_setting in pr_settings:
                if pr_setting in possible_settings:
                    #if not isinstance(pr_settings[pr_setting], possible_settings[pr_setting]):
                    #    logger.warning("invalid type in yaml")
                    setattr(self, pr_setting, pr_settings[pr_setting])
                else:
                    return_text = "Did not understand variable '{}' for {}".format(pr_setting, self.subreddit_name)
                    print(return_text)

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
            tr_sub.update_from_yaml(force_update=False)
        return tr_sub

    def get_author_summary(self, author_name: str) -> str:
        if author_name.startswith('u/'):
            author_name = author_name.replace("u/", "")

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
    tick = datetime.now()
    possible_reposts = s.query(SubmittedPost) \
        .filter(SubmittedPost.flagged_duplicate.is_(False),
                SubmittedPost.subreddit.ilike(tr_sub.subreddit_name),
                SubmittedPost.time_utc > recent_post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins,
                SubmittedPost.time_utc < recent_post.time_utc,  # posts not after post in question
                SubmittedPost.id != recent_post.id,  # not the same post id
                SubmittedPost.author == recent_post.author,  # same author
                SubmittedPost.counted_status != 0) \
        .order_by(SubmittedPost.time_utc) \
        .all()

    # Filter possible reposts (some maybe removed by automoderator or within grace period) - can't do in database
    most_recent_reposts = []
    for possible_repost in possible_reposts:
        logger.info("possible repost of: {0}... http://redd.it/{1} {2} already counted? {3}".format(
            possible_repost.title[0:20],
            possible_repost.id,
            datetime.now().replace(tzinfo=timezone.utc) - possible_repost.time_utc.replace(tzinfo=timezone.utc),
            possible_repost.counted_status))

        # Need to check if it counts if not already checked:
        if possible_repost.counted_status == -1:  # so it was not previously checked
            possible_repost.counted_status, result = check_for_post_exemptions(tr_sub, possible_repost)  #check it

        if possible_repost.counted_status == 1:  # counted status is 1 means this is counted towards cap
            # check for grace period exception (post was deleted and poster reposted within grace period)
            self_deleted = False if possible_repost.get_api_handle().author else True
            if self_deleted and recent_post.time_utc.replace(tzinfo=timezone.utc) \
                    - possible_repost.time_utc.replace(tzinfo=timezone.utc) < tr_sub.grace_period_mins:
                possible_repost.counted_status = 0
            else:
                most_recent_reposts.append(possible_repost)
        s.add(possible_repost)  # update database
    s.commit()
    logger.info("----------------total {} max {} query time: {}".format(
        len(most_recent_reposts),
        tr_sub.max_count_per_interval,
        datetime.now() - tick
    ))
    return most_recent_reposts


def check_for_post_exemptions(tr_sub, recent_post):
    # check if removed
    banned_by = recent_post.get_api_handle().banned_by
    if ((tr_sub.ignore_AutoModerator_removed and banned_by == "AutoModerator")
            or (tr_sub.ignore_moderator_removed and banned_by in tr_sub.subreddit_mods)):
        return 0, "post is removed"

    # check if oc exempt:
    if tr_sub.exempt_oc and recent_post.get_api_handle().is_original_content:
        return 0, "oc exempt"

    # Check if any post type restrictions
    is_self = recent_post.get_api_handle().is_self
    if is_self is True and tr_sub.exempt_self_posts is True:
        return 0, "self_post_exempt"
    if is_self is not True and tr_sub.exempt_link_posts is True:
        return 0, "link_post_exempt"

    # check if flair-exempt
    author_flair = recent_post.get_api_handle().author_flair_text
    # add CSS class to author_flair
    if author_flair and recent_post.get_api_handle().author_flair_css_class:
        author_flair = author_flair + recent_post.get_api_handle().author_flair_css_class

    # Flair keyword exempt
    if tr_sub.author_exempt_flair_keyword and isinstance(tr_sub.author_exempt_flair_keyword, str) \
            and author_flair and tr_sub.author_exempt_flair_keyword in author_flair:
        return 0, "flair exempt {}".format(author_flair)

    # Not-flair-exempt keyword (Only restrict certain flairs)
    if tr_sub.author_not_exempt_flair_keyword \
            and ((author_flair and tr_sub.author_not_exempt_flair_keyword not in author_flair) or not author_flair):
        return 0, "flair not exempt {}".format(author_flair)

    # check if title keyword exempt:
    if tr_sub.title_exempt_keyword and tr_sub.title_exempt_keyword.lower() in recent_post.title.lower():
        return 0, "title keyword exempt {}".format(tr_sub.title_exempt_keyword)

    # title keywords only to restrict:
    if tr_sub.title_not_exempt_keyword:
        if (isinstance(tr_sub.title_not_exempt_keyword, str)
            and tr_sub.title_not_exempt_keyword.lower() not in recent_post.title.lower()) or \
                (isinstance(tr_sub.title_not_exempt_keyword, list)
                 and all(x not in recent_post.title for x in tr_sub.title_not_exempt_keyword)):
            return 0, "title keyword not exempt {}".format(tr_sub.title_exempt_keyword)

    # Ignore posts by mods
    if tr_sub.exempt_moderator_posts is True and recent_post.author in tr_sub.subreddit_mods:
        return 0, "moderator exempt"

    return 1, "no exemptions"


def look_for_rule_violations():
    global reddit_client
    global watched_subs
    authors_to_watch_for_subreddit = dict()
    logger.debug("querying recent post(s)")
    recent_posts = s.query(SubmittedPost).filter(
        SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(hours=LOOK_BACK_INTERVAL_HRS),
        SubmittedPost.flagged_duplicate.is_(False),
        SubmittedPost.reviewed.is_(False),
        SubmittedPost.banned_by.is_(None)) \
        .order_by(desc(SubmittedPost.time_utc)) \
        .limit(100).all()
    logger.info("checking for violations...")

    index = 0
    for index, recent_post in enumerate(recent_posts):
        if (index+1) % 20 == 0:
            s.commit()
            logger.info("$$$ -{0} current time".format(datetime.now(pytz.utc).replace(tzinfo=timezone.utc) - recent_post.time_utc.replace(tzinfo=timezone.utc)))
            logger.debug("           %d of %d" % (index, len(recent_posts)))

        # Load subreddit settings
        subreddit_name = recent_post.subreddit.lower()
        if subreddit_name not in watched_subs:
            tr_sub = update_list_with_subreddit(subreddit_name)
            if tr_sub:
                if tr_sub.last_updated < datetime.now() - timedelta(hours=24):
                    purge_old_records_by_subreddit(tr_sub)
                    tr_sub.update_from_yaml(force_update=True)
                    s.add(tr_sub)
                    s.commit()
        tr_sub = watched_subs[subreddit_name]

        # Check if they're on the watchlist
        subreddit_author = s.query(SubAuthor).get((subreddit_name, recent_post.author))
        if subreddit_author and subreddit_author.next_eligible.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc):
            was_successful = recent_post.mod_remove()
            if was_successful:
                try:
                    comment = recent_post.reply("You are posting too frequently for the subreddit's rules. "
                                                "You are currently not eligible to post until {0} UTC. "
                                                "Please message the [moderators]"
                                                "(https://www.reddit.com/message/compose?to=%2Fr%2F{1}"
                                                "&subject=problem%20with%20bot) if you feel this is in error."
                                                .format(subreddit_author.next_eligible, subreddit_name))
                    comment.mod.distinguish(sticky=True)
                except (praw.exceptions.APIException, prawcore.exceptions.Forbidden) as e:
                    logger.warning('something went wrong in creating comment %s', str(e))
                subreddit_author.update_blacklisted_post_list(recent_post.id, recent_post.time_utc)
                recent_post.reviewed = True
                recent_post.flagged_duplicate = True
                recent_post.counted_status = 3
                logger.info("post removed - prior to eligibility for user {0} {1} {2} {3}".format(recent_post.author, recent_post.get_url(), recent_post.subreddit, recent_post.title))
                s.add(subreddit_author)
                s.add(recent_post)
                continue
            else:
                # Maybe recheck permissions if not allowed to remove posts
                tr_sub.update_from_yaml(force_update=True)

        # See which authors we can skip - find any authors in the min post interval that have posted more than once
        if subreddit_name not in authors_to_watch_for_subreddit:
            authors_tuple = s.query(SubmittedPost.author, func.count(SubmittedPost.author).label('qty')) \
                .filter(SubmittedPost.subreddit.ilike(subreddit_name)) \
                .filter(
                SubmittedPost.time_utc > datetime.now(pytz.utc) - tr_sub.min_post_interval + tr_sub.grace_period_mins) \
                .group_by(SubmittedPost.author).order_by(desc('qty')).all()
            authors_to_watch_for_subreddit[subreddit_name] = dict((x, y) for x, y in authors_tuple)

        if subreddit_name not in watched_subs:
            print("CANNOT FIND SUBREDDIT!!! {0}".format(recent_post.subreddit))
            continue


        # Shortcut - ignore authors in the alst time period
        # careful though!! it's from the most recent post not the actual post time!
        if subreddit_name in authors_to_watch_for_subreddit\
                and recent_post.author in authors_to_watch_for_subreddit[subreddit_name] \
                and recent_post.time_utc.replace(tzinfo=timezone.utc) > \
                datetime.now(pytz.utc).replace(tzinfo=timezone.utc) - timedelta(minutes=30):
            author_count = authors_to_watch_for_subreddit[subreddit_name][recent_post.author]
            if author_count <= tr_sub.max_count_per_interval:
                recent_post.reviewed = True
                s.add(recent_post)
                continue

        # check for post exemptions
        counted_status, result = check_for_post_exemptions(tr_sub, recent_post)
        logger.info("{0}-Checking '{1}...' by '{2}' http://redd.it/{3} subreddit:({4}): {5}".format(
            index,
            recent_post.title[0:20],
            recent_post.author,
            recent_post.id,
            recent_post.subreddit,
            result
        ))
        if counted_status == 0:
            recent_post.counted_status = 0
            recent_post.reviewed = True
            s.add(recent_post)
            continue
        associated_reposts = find_previous_posts(tr_sub, recent_post)
        verified_reposts_count = len(associated_reposts)

        # Now check if actually went over threshold
        if verified_reposts_count >= tr_sub.max_count_per_interval:
            logger.info("----------------post time -{0} | interval {1}  after {2} sub:{3}".format(
                datetime.now().replace(tzinfo=timezone.utc) - recent_post.time_utc.replace(tzinfo=timezone.utc),
                tr_sub.min_post_interval,
                recent_post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins,
                recent_post.subreddit, recent_post.time_utc))

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
    return index


def do_requested_action_for_valid_reposts(tr_sub: TrackedSubreddit, recent_post: SubmittedPost,
                                          most_recent_reposts: List[SubmittedPost]):
    possible_repost = most_recent_reposts[-1]
    if tr_sub.comment:
        make_comment(tr_sub, recent_post, most_recent_reposts,
                     tr_sub.comment, distinguish=tr_sub.distinguish, approve=tr_sub.approve,
                     lock_thread=tr_sub.lock_thread, stickied=tr_sub.comment_stickied)
    if tr_sub.modmail:
        message = tr_sub.modmail
        if message is True:
            message = "Repost that violates rules: [{title}]({url}) by [{author}](/u/{author})"
        send_modmail(tr_sub, recent_post,
                     possible_repost, message)
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


def check_for_actionable_violations(tr_sub: TrackedSubreddit, recent_post: SubmittedPost,
                                    most_recent_reposts: List[SubmittedPost]):
    possible_repost = most_recent_reposts[-1]
    tick = datetime.now()
    other_spam_by_author = s.query(SubmittedPost).filter(
        SubmittedPost.flagged_duplicate.is_(True),
        SubmittedPost.author == recent_post.author,
        SubmittedPost.subreddit.ilike(tr_sub.subreddit_name),
        SubmittedPost.time_utc < recent_post.time_utc) \
        .all()

    logger.info("Author {0} had {1} rule violations. Banning if at least {2} - query time took: {3}"
                .format(recent_post.author, len(other_spam_by_author), tr_sub.ban_threshold_count, datetime.now() - tick))

    if len(other_spam_by_author) >= tr_sub.ban_threshold_count:
        num_days = tr_sub.ban_duration_days
        if num_days < 1:
            num_days = 1
        if num_days > 998:
            num_days = 0

        str_prev_posts = ",".join([" [{0}]({1})".format(a.id, a.get_comments_url()) for a in other_spam_by_author])

        ban_message = "For this subreddit, you have posted too soon too many times (threshold of {0}): {1}.".format(
            tr_sub.ban_threshold_count, str_prev_posts)
        time_next_eligible = datetime.now(pytz.utc) + timedelta(days=num_days)
        if num_days > 0:
            ban_message += "\n\nYour ban will last {0} days from this message, ending at {1} UTC. " \
                           "**Repeat infractions result in a permanent ban!**" \
                           "".format(num_days, time_next_eligible)
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
            if tr_sub.notify_about_spammers:
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

                send_modmail(tr_sub, recent_post,
                             possible_repost, "\n\n".join(response_lines))
            #Add to the watch list
            subreddit_author = s.query(SubAuthor).get((tr_sub.subreddit_name, recent_post.author))
            if not subreddit_author:
                subreddit_author = SubAuthor(tr_sub.subreddit_name, recent_post.author)
            subreddit_author.next_eligible = time_next_eligible
            s.add(subreddit_author)
            s.commit()


def populate_tags(input_text, recent_post, tr_sub=None, prev_post=None, prev_posts=None):
    if not isinstance(input_text, str):
        print("error: {0} is not a string".format(input_text))
        return "error: {0} is not a string"
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
        input_text = input_text.replace("{timedelta}", humanize.naturaltime(datetime.now() - prev_post.time_utc))
    if recent_post:
        input_text = input_text.replace("{author}", recent_post.author)
        input_text = input_text.replace("{title}", recent_post.title)
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
    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden, AttributeError):
        logger.warning('something went wrong in sending modmail')


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


def handle_dm_command(subreddit_name, author_name, command, parameters):
    response = ""
    subreddit_name = subreddit_name[2:] if subreddit_name.startswith('r/') else subreddit_name
    subreddit_name = subreddit_name[3:] if subreddit_name.startswith('/r/') else subreddit_name
    command = command[1:] if command.startswith("$") else command
    tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
    if not tr_sub:
        return "Error retrieving information for /r/{}".format(subreddit_name)
    moderators = tr_sub.subreddit_mods
    if author_name not in moderators and author_name != BOT_OWNER:
        return "You do not have permission to do this. Are you sure you are a moderator of {}?"\
            .format(subreddit_name)

    if command == "summary":
        author_name_to_check = parameters[0] if parameters else None
        if not author_name_to_check:
            return "No author name given"
        return tr_sub.get_author_summary(author_name_to_check)
    elif command == "stats":
        return tr_sub.get_sub_stats()
    elif command == "hallpass":
        author_name_to_check = parameters[0] if parameters else None
        if not author_name_to_check:
            return "No author name given"
        subreddit_author = s.query(SubAuthor).get((subreddit_name, author_name_to_check))
        if not subreddit_author:
            subreddit_author = SubAuthor(tr_sub.subreddit_name, author_name_to_check)
        subreddit_author.hall_pass = 1
        s.add(subreddit_author)
        return_text = "User {} has been granted a hall pass. "\
                      "This means the next post by the user in this subreddit will not be automatically removed."\
                      .format(author_name_to_check)
        return return_text

    elif command == "update":
        worked, status = tr_sub.update_from_yaml(force_update=True)
        reply_text = "Received message to update config for {0}.  See the output below. " \
                     "If you get a 404 error, it means that the config page needs to be created. " \
                     "If you get a 503 error, it means the bot doesn't have wiki permissions. " \
                     "If you get a 'yaml' error, there is an error in your syntax. " \
                     "Please message [/r/moderatelyhelpfulbot](https://www.reddit.com/" \
                     "message/compose?to=%2Fr%2Fmoderatelyhelpfulbot) if you have any questions \n\n" \
                     "Update report: \n\n >{1}" \
            .format(subreddit_name, status, )
        bot_owner_message = "subreddit: {0}\n\nrequestor: {1}\n\nreport: {2}" \
            .format(subreddit_name, author_name, status)
        reddit_client.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
        s.add(tr_sub)
        s.commit()
        return reply_text
    else:
        return "I did not understand that command"


def handle_direct_messages():
    # Reply to pms or
    global watched_subs
    for message in reddit_client.inbox.unread(limit=None):
        logger.info("got this email author:{} subj:{}  body:{} ".format(message.author, message.subject, message.body))

        # Get author name, message_id if available
        author_name = message.author.name if message.author else None
        message_id = reddit_client.comment(message.id).link_id if message.was_comment else message.name
        body_parts = message.body.split(' ')
        command = body_parts[0].lower() if len(body_parts) > 0 else None
        subreddit_name = message.subject.replace("re: ", "") if command else None
        print("command: {}".format(command))
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
        # Respond to an invitation to moderate
        elif message.subject.startswith('invitation to moderate'):
            mod_mail_invitation_to_moderate(message)
        elif command in ("summary", "update", "stats") or command.startswith("$"):
            subreddit_name = message.subject.lower().replace("re: ", "")
            response = handle_dm_command(subreddit_name, author_name, command, body_parts[1:])
            message.reply(response[:999])
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


def mod_mail_invitation_to_moderate(message):
    if ACCEPTING_NEW_SUBS:
        subreddit_name = message.subject.replace("invitation to moderate /r/", "")
        sub = reddit_client.subreddit(subreddit_name)
        try:
            sub.mod.accept_invite()
        except praw.exceptions.APIException:
            message.reply("Error: Invite message has been rescinded?")

        message.reply("Hi, thank you for inviting me!  I will start working now. Please make sure I have a config. "
                      "It should be at https://www.reddit.com/r/{0}/wiki/moderatelyhelpfulbot . "
                      "You may need to create it. You can find examples at "
                      "https://www.reddit.com/r/moderatelyhelpfulbot/wiki/index . "
                      .format(subreddit_name))
    else:
        message.reply("Unfortunately ModeratelyHelpfulBot is not **automatically** accepting new subreddits at this time"
                      "due to high usage load.")
    message.mark_read()

def handle_modmail_messages():
    global watched_subs
    for convo in reddit_client.subreddit('all').modmail.conversations(state="all", sort='unread', limit=15):
        last_updated_dt = iso8601.parse_date(convo.last_updated)
        if last_updated_dt < datetime.now(timezone.utc) - timedelta(hours=24):
            convo.read()
            continue
        author_name = convo.authors[0].name
        subreddit_name = convo.owner.display_name
        if subreddit_name not in watched_subs:
            update_list_with_subreddit(subreddit_name)
        tr_sub = watched_subs[subreddit_name]
        if not tr_sub:
            continue
        # Ignore conversations initiated by Automoderator
        if author_name == "AutoModerator":
            convo.read()
            continue
        # Ignore conversations initiated by moderators
        if author_name in tr_sub.subreddit_mods:
            convo.read()
            continue
        print(
            'test000000000000000000000000000000' + convo.subject + "  " + subreddit_name + " " + str(convo.last_unread))
        if convo.num_messages == 1:
            if check_actioned(convo.id):
                convo.read()
                continue
            if tr_sub.modmail_all_reply:
                response = populate_tags(tr_sub.modmail_all_reply, None, tr_sub=tr_sub)
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
                    try:
                        convo.reply(response, internal=tr_sub.modmail_no_posts_reply_internal)
                    except prawcore.exceptions.BadRequest:
                        pass
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
        tr_sub.update_from_yaml(force_update=False)

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

    count = 0
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
            s.add(post)
            count += 1
    logger.info('found {0} posts'.format(count))
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


def worker2():
    for submission in reddit_client.subreddit('mod').stream.submissions():
        q.put(submission)
    time.sleep(5)


def worker():
    already_seen = []
    """
    try:
        already_seen = pickle.load(open("already_seen.pickle", "rb"))
    except IOError:
        
    """

    limit = 1000
    while True:
        count = 0

        for submission in reddit_client.subreddit('mod').new(limit=limit):

            submission_id = submission.id
            if submission_id in already_seen:
                # print('skipping {0}'.format(submission_id))
                continue
            already_seen.append(submission_id)
            q.put(submission)
            count += 1
            #limit = 500
        print('###added {0} new posts'.format(count))
        if len(already_seen) > 10000:
            already_seen = already_seen[:(10000 - len(already_seen))]

        time.sleep(30)

def main_loop():
    global watched_subs
    load_settings()
    purge_old_records()
    tr_subs = dict()
    # update_list_with_all_active_subs()
    # threading.Thread(target=worker, daemon=True).start()
    while True:
        # moderate_debates()
        # scan_comments_for_activity()
        # flag_all_submissions_for_activity()
        # recalculate_active_submissions()
        print('start_loop')


        # subs_to_update = check_new_submissions2()
        # print("substoupdate:")
        # print(subs_to_update)

        #check_spam_submissions()  - don't need to anymore?

        #check_new_submissions3()

        check_new_submissions2a()

        start = datetime.now()
        last_index = look_for_rule_violations()

        #only do this if not too busy
        if last_index < 30:
            check_spam_submissions()
        print("$$$checking rule violations took this long", datetime.now()-start)

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


def utcize(dt):
    return dt.replace(tzinfo=timezone.utc)

def utcnow():
    return datetime.now().replace(tzinfo=timezone.utc)

main_loop()

