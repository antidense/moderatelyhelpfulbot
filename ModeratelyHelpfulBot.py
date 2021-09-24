#!/usr/bin/env python3.7
import logging
from datetime import datetime, timedelta, timezone
from typing import List

import humanize
import iso8601
import praw
import prawcore
import pytz
import yaml
import re
from praw import exceptions
from praw.models import Submission
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from praw.models.listing.generator import ListingGenerator
from praw.models.listing.mixins.redditor import SubListing
from sqlalchemy.orm import sessionmaker
from enum import Enum

from settings import BOT_NAME, BOT_PW, CLIENT_ID, CLIENT_SECRET, BOT_OWNER, DB_ENGINE


"""
To do list:
asyncio 
incorporate toolbox? https://www.reddit.com/r/nostalgia/wiki/edit/toolbox check usernotes?
upgrade python and praw version
clean actioned comments after 3 months
fix logging
"""

MINOR_KWS = []

#ASL_REGEX =r"((?P<age>[0-9]){2}[/ \\]?(?P<g>[mMFf]{1}))|((?P<g2>[mMFf]{1})[/ \\]?(?P<age2>[0-9]){2})"
ASL_REGEX = r"((?P<age>[0-9]{2})([/ \\-]?|(\]? \[))(?P<g>[mMFf]{1}))|((?P<g2>[mMFf]{1})([/ \\-]?|(\]? \[))(?P<age2>[0-9]{2}))"
# Non binary?

# Set up database
engine = create_engine(DB_ENGINE)
Base = declarative_base(bind=engine)

# Set up PRAW
REDDIT_CLIENT = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, password=BOT_PW,
                            user_agent="ModeratelyHelpfulBot v0.4", username=BOT_NAME)

# Set up some global variables
ACCEPTING_NEW_SUBS = False
LINK_REGEX = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
REDDIT_LINK_REGEX = r'r/([a-zA-Z0-9_]*)/comments/([a-z0-9_]*)/([a-zA-Z0-9_]{0,50})'
RESPONSE_TAIL = ""
MAIN_SETTINGS = dict()
WATCHED_SUBS = dict()
SUBWIKI_CHECK_INTERVAL_HRS = 24



class PostedStatus(Enum):
    SELF_DEL = "self-deleted"
    UP = "up"
    MOD_RM = "mod-removed"
    AUTOMOD_RM = "AutoMod-removed"
    MHB_RM = "MHB-removed"
    BOT_RM = "Bot-removed"
    SPAM_FLT = "Spam-filtered"
    UNKNOWN = "Unknown status"
    FH_RM = "Flair_Helper removed"


class CountedStatus(Enum):
    NOT_CHKD = -1   # include in search
    PREV_EXEMPT = 0  # Previously the code for exemption, switched to 2
    COUNTS = 1  # include in search
    EXEMPTED = 2  # don't include in search  0 --> CHANGE to 2*****  no longer use, use more specific
    BLKLIST = 3  # don't include in search
    HALLPASS = 4  # don't include in search
    FLAGGED = 5
    SPAMMED_EXMPT = 6
    AM_RM_EXEMPT = 7
    MOD_RM_EXEMPT = 8
    OC_EXEMPT = 9
    SELF_EXEMPT = 10
    LINK_EXEMPT = 11
    FLAIR_EXEMPT = 12
    FLAIR_NOT_EXEMPT = 13
    TITLE_KW_EXEMPT = 14
    TITLE_KW_NOT_EXEMPT = 15
    MODPOST_EXEMPT = 16
    GRACE_PERIOD_EXEMPT = 17
    FLAIR_HELPER = 18


# For messaging subreddits that use bot
class Broadcast(Base):
    __tablename__ = 'Broadcast'
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    text = Column(String(191), nullable=True)
    subreddit = Column(String(191), nullable=True)
    sent = Column(Boolean, nullable=True)

    def __init__(self, post):
        self.id = post.id




class TrackedAuthor(Base):
    __tablename__ = 'TrackedAuthor'
    author_name = Column(String(21), nullable=True, primary_key=True)
    nsfw_pct = Column(Integer)
    last_calculated = Column(DateTime, nullable=True)
    created_date = Column(DateTime, nullable=False)
    gender = Column(String(5), nullable=True)
    num_history_items = Column(Integer)
    has_nsfw_post = Column(String(10), nullable=True)
    age = Column(Integer)
    api_handle = None

    def __init__(self, author_name):
        self.author_name = author_name
        self.nsfw_pct = -1
        self.created_date = datetime.now(pytz.utc)
        self.num_history_items = -1
        self.has_nsfw_post = None

    def get_api_handle(self):
        if not self.api_handle:
            self.api_handle = REDDIT_CLIENT.redditor(self.author_name)
            return self.api_handle
        else:
            return self.api_handle

    def calculate_nsfw(self):
        intended_total =50
        total = 0
        count = 0
        age = 0
        api_handle: praw.models.Redditor = self.get_api_handle()
        sublisting: SubListing = api_handle.comments
        comments_generator: ListingGenerator = sublisting.new(limit=intended_total)
        postlisting: SubListing = api_handle.submissions
        post_generator: ListingGenerator = postlisting.new(limit=10)
        try:
            comments: List[praw.models.reddit.comment.Comment] = list(comments_generator)
            for comment in comments:
                total += 1
                if comment.subreddit.over18:
                    count += 1
                if not age:
                    age = get_age(comment.body)
                    if age > 10:
                        self.age = age
            posts: List[praw.models.reddit.Submission.submission] = list(post_generator)
            for post in posts:
                total += 1
                if post.over_18:
                    count += 1
                    if not post.is_self:
                        self.has_nsfw_post = post.id
                if not age:
                    age = get_age(post.title)
                    if age > 10:
                        self.age = age
            self.nsfw_pct = (count*100)/total
        except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden):
            pass
        self.age = age
        self.last_calculated = datetime.now(pytz.utc)
        self.num_history_items = total
        return self.nsfw_pct

        # In the future,  look for: /r/dirtypenpals   DDLGPersonals   cglpersonals  littlespace dirtyr4r ddlg

def get_age(input_text: str):
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
            matches = re.match(r"[iI]((')|( a))?m (?P<age>[0-9]{2})", input_text)
            if matches:
                if matches.group('age'):
                    age = int(matches.group('age'))
    # print(f"age: {age}  text:{input_text} ")
    return age

class SubmittedPost(Base):
    __tablename__ = 'RedditPost'
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    author = Column(String(21), nullable=True)
    submission_text = Column(String(191), nullable=True)
    time_utc = Column(DateTime, nullable=False)
    subreddit_name = Column(String(21), nullable=True)
    # banned_by = Column(String(21), nullable=True)  # Can delete this now ---------
    flagged_duplicate = Column(Boolean, nullable=True)
    pre_duplicate = Column(Boolean, nullable=True)
    # self_deleted = Column(Boolean, nullable=True)  # Can delete this now -------
    reviewed = Column(Boolean, nullable=True)  # could be combined into an Enum: reviewed, flagged_duplicate, pre_duplicate
    last_checked = Column(DateTime, nullable=False)  # redundant with reviewed?  can't use as inited with old date
    bot_comment_id = Column(String(10), nullable=True)  # wasn't using before but using now
    is_self = Column(Boolean, nullable=True)  # Can delete this now----------
    # removed_status = Column(String(21), nullable=True)  # not used
    post_flair = Column(String(21), nullable=True)
    author_flair = Column(String(42), nullable=True)
    counted_status = Column(Integer)
    # newly added
    response_time = Column(DateTime, nullable=True)  # need to add everywhere
    review_debug = Column(String(191), nullable=True)
    flushed_to_log = Column(Boolean, nullable=False)
    nsfw_repliers_checked = Column(Boolean, nullable=False)  # REMOVE!!
    nsfw_last_checked = Column(DateTime, nullable=True)

    api_handle = None

    def __init__(self, submission: Submission, save_text: bool = False):
        self.id = submission.id
        self.title = submission.title[0:190]
        self.author = str(submission.author)
        if save_text:
            self.submission_text = submission.selftext[0:190]
        self.time_utc = datetime.utcfromtimestamp(submission.created_utc)
        self.subreddit_name = str(submission.subreddit).lower()
        self.flagged_duplicate = False
        self.reviewed = False
        self.banned_by = None
        self.api_handle = submission
        self.pre_duplicate = False
        self.self_deleted = False
        self.is_self = submission.is_self
        self.counted_status = CountedStatus.NOT_CHKD.value
        self.post_flair = submission.link_flair_text
        self.author_flair = submission.author_flair_text
        self.response_time = None
        self.nsfw_last_checked = self.time_utc
        self.nsfw_repliers_checked = False

        self.age = get_age(self.title)
        if 25 > self.age > 13 and self.subreddit_name == "needafriend":
            self.post_flair = "strict sfw"

    def get_url(self) -> str:
        return f"http://redd.it/{self.id}"

    def get_comments_url(self) -> str:
        return f"https://www.reddit.com/r/{self.subreddit_name}/comments/{self.id}"

    def get_removed_explanation(self):
        if not self.bot_comment_id:
            return None
        comment = REDDIT_CLIENT.comment(self.bot_comment_id)
        if comment and comment.body:
            return comment.body
        else:
            return None

    def get_removed_explanation_url(self):
        if not self.bot_comment_id:
            return None
        return f"https://www.reddit.com/r/{self.subreddit_name}/comments/{self.id}//{self.bot_comment_id}"

    def get_api_handle(self) -> praw.models.Submission:
        if not self.api_handle:
            self.api_handle = REDDIT_CLIENT.submission(id=self.id)
            return self.api_handle
        else:
            return self.api_handle

    def mod_remove(self) -> bool:
        try:
            self.get_api_handle().mod.remove()
            return True
        except praw.exceptions.APIException:
            logger.warning(f'something went wrong removing post: http://redd.it/{self.id}')
            return False
        except (prawcore.exceptions.Forbidden, prawcore.exceptions.ServerError):
            logger.warning(f'I was not allowed to remove the post: http://redd.it/{self.id}')
            return False

    def reply(self, response, distinguish=True, approve=False, lock_thread=True):
        try:
            # first try to lock thread - useless to make a comment unless it's possible
            if lock_thread:
                self.get_api_handle().mod.lock()
            comment = self.get_api_handle().reply(response)
            if distinguish:
                comment.mod.distinguish()
            if approve:
                comment.mod.approve()
            return comment
        except praw.exceptions.APIException:
            logger.warning(f'Something went wrong with replying to this post: http://redd.it/{self.id}')
            return False
        except (prawcore.exceptions.Forbidden, prawcore.exceptions.ServerError):
            logger.warning(f'Something with replying to this post:: http://redd.it/{self.id}')
            return False

    def get_posted_status(self, get_removed_info=False) -> PostedStatus:
        _ = self.get_api_handle()
        self.self_deleted = False if self.api_handle.author else True
        self.banned_by = self.api_handle.banned_by
        if not self.banned_by and not self.self_deleted:
            return PostedStatus.UP
        elif self.banned_by:
            if self.banned_by is True:
                return PostedStatus.SPAM_FLT
            if not self.bot_comment_id and get_removed_info:  # make sure to commit to db
                top_level_comments = list(self.get_api_handle().comments)
                for c in top_level_comments:
                    if hasattr(c, 'author') and c.author and c.author.name == self.banned_by:
                        self.bot_comment_id = c.id
                        break
            if self.banned_by == "AutoModerator":
                return PostedStatus.AUTOMOD_RM
            elif self.banned_by == "Flair_Helper":
                return PostedStatus.FH_RM
            elif self.banned_by == BOT_NAME:
                return PostedStatus.MHB_RM
            elif "bot" in self.banned_by.lower():
                return PostedStatus.BOT_RM
            else:
                return PostedStatus.MOD_RM
        elif self.self_deleted:
            return PostedStatus.SELF_DEL
        else:
            print(f"unknown status: {self.banned_by}")
            return PostedStatus.UNKNOWN

    def update_status(self, reviewed=None, flagged_duplicate=None, counted_status=None):
        if reviewed is not None:
            self.reviewed = reviewed
            if not self.response_time:
                self.response_time = datetime.now(pytz.utc)
        if counted_status is not None:
            self.counted_status = counted_status.value
        if flagged_duplicate is not None:
            self.flagged_duplicate = flagged_duplicate
            if flagged_duplicate is True:
                self.counted_status = CountedStatus.FLAGGED.value
        self.last_checked = datetime.now(pytz.utc)
        self.response_time = datetime.now(pytz.utc)-self.time_utc.replace(tzinfo=timezone.utc)

class CommonPost():  # did not (Base) yet!!!  use for tracking bot spam
    __tablename__ = 'CommonPost'
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    author = Column(String(21), nullable=True)
    time_utc = Column(DateTime, nullable=False)
    subreddit_name = Column(String(21), nullable=True)

    api_handle = None

    def __init__(self, submission: Submission, save_text: bool = False):
        self.id = submission.id
        self.title = submission.title[0:190]
        self.author = str(submission.author)
        self.time_utc = datetime.utcfromtimestamp(submission.created_utc)
        self.subreddit_name = str(submission.subreddit).lower()

    def get_url(self) -> str:
        return f"http://redd.it/{self.id}"

    def get_comments_url(self) -> str:
        return f"https://www.reddit.com/r/{self.subreddit_name}/comments/{self.id}"

    def get_api_handle(self) -> praw.models.Submission:
        if not self.api_handle:
            self.api_handle = REDDIT_CLIENT.submission(id=self.id)
            return self.api_handle
        else:
            return self.api_handle


class SubAuthor(Base):
    __tablename__ = 'SubAuthors'
    subreddit_name = Column(String(21), nullable=False, primary_key=True)
    author_name = Column(String(21), nullable=False, primary_key=True)
    currently_banned = Column(Boolean, default=False)
    ban_count = Column(Integer, nullable=True, default=0)
    currently_blacklisted = Column(Boolean, nullable=True)
    violation_count = Column(Integer, default=0)
    post_ids = Column(UnicodeText, nullable=True)
    blacklisted_post_ids = Column(UnicodeText, nullable=True)  # to delete
    last_updated = Column(DateTime, nullable=True, default=datetime.now())  #NOT UTC!!!!!!!!!!
    next_eligible = Column(DateTime, nullable=True, default=datetime(2019, 1, 1, 0, 0))
    ban_last_failed = Column(DateTime, nullable=True)
    hall_pass = Column(Integer, default=0)
    last_valid_post = Column(String(10))

    def __init__(self, subreddit_name: str, author_name: str):
        self.subreddit_name = subreddit_name
        self.author_name = author_name


class TrackedSubreddit(Base):
    __tablename__ = 'TrackedSubs'
    subreddit_name = Column(String(21), nullable=False, primary_key=True)
    checking_mail_enabled = Column(Boolean, nullable=True)  #don't need this?
    settings_yaml_txt = Column(UnicodeText, nullable=True)
    settings_yaml = None
    last_updated = Column(DateTime, nullable=True)
    last_error_msg = Column(DateTime, nullable=True)  # not used
    save_text = Column(Boolean, nullable=True)
    max_count_per_interval = Column(Integer, nullable=False, default=1)
    min_post_interval_mins = Column(Integer, nullable=False, default=60 * 72)
    bot_mod = Column(String(21), nullable=True, default=None)
    ban_ability = Column(Integer, nullable=False, default=-1)
    # -2 -> bans enabled but no perms
    # -1 -> unknown
    # 0 -> bans not enabled
    # 1 -> bans enabled (perma)
    # 2 -> bans enabled (not perma)
    mm_convo_id = Column(String(10), nullable=True, default=None)
    is_nsfw = Column(Boolean, nullable=False, default=0)

    subreddit_mods = []
    rate_limiting_enabled = False
    min_post_interval_hrs = 72
    min_post_interval_txt = ""
    min_post_interval = timedelta(hours=72)
    grace_period = timedelta(minutes=30)
    ban_duration_days = 0
    ignore_AutoModerator_removed = True
    ignore_moderator_removed = True
    ban_threshold_count = 5
    notify_about_spammers = False
    author_exempt_flair_keyword = None
    author_not_exempt_flair_keyword = None
    title_exempt_keyword = None
    action = None
    modmail = None
    message = None
    report_reason = None
    comment = None
    distinguish = True
    exempt_self_posts = False
    exempt_link_posts = False
    exempt_moderator_posts = True
    exempt_oc = False
    modmail_posts_reply = True
    modmail_no_link_reply = False
    modmail_no_posts_reply = None
    modmail_no_posts_reply_internal = False
    modmail_notify_replied_internal = True
    modmail_auto_approve_messages_with_links = False
    modmail_all_reply = None
    modmail_removal_reason_helper = False
    approve = False
    blacklist_enabled = True
    lock_thread = True
    comment_stickied = False
    title_not_exempt_keyword = None
    canned_responses = {}
    api_handle = None

    def __init__(self, subreddit_name: str):
        self.subreddit_name = subreddit_name.lower()
        self.save_text = False
        self.last_updated = datetime(2019, 1, 1, 0, 0)
        self.update_from_yaml(force_update=True)
        self.settings_revision_date = None
        self.api_handle = REDDIT_CLIENT.subreddit(self.subreddit_name)

    def get_mods_list(self, subreddit_handle=None) -> List[str]:
        self.api_handle = REDDIT_CLIENT.subreddit(self.subreddit_name) if not self.api_handle else self.api_handle
        try:
            return list(moderator.name for moderator in self.api_handle.moderator())
        except prawcore.exceptions.NotFound:
            return []

    def update_from_yaml(self, force_update: bool = False) -> (Boolean, String):
        return_text = "Updated Successfully!"
        self.api_handle = REDDIT_CLIENT.subreddit(self.subreddit_name) if not self.api_handle else self.api_handle
        self.is_nsfw = self.api_handle.over18
        self.ban_ability = -1
        self.subreddit_mods = self.get_mods_list(subreddit_handle=self.api_handle)

        if force_update or self.settings_yaml_txt is None:
            try:
                logger.warning('accessing wiki config %s' % self.subreddit_name)
                wiki_page = REDDIT_CLIENT.subreddit(self.subreddit_name).wiki[BOT_NAME]
                if wiki_page:
                    self.settings_yaml_txt = wiki_page.content_md
                    self.settings_revision_date = wiki_page.revision_date
                    if wiki_page.revision_by:
                        self.bot_mod = wiki_page.revision_by.name
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
                'max_count_per_interval': "int",
                'ignore_AutoModerator_removed': "bool",
                'ignore_moderator_removed': "bool",
                'ban_threshold_count': "int",
                'notify_about_spammers': "bool;int",
                'ban_duration_days': "int",
                'author_exempt_flair_keyword': "str;list",
                'author_not_exempt_flair_keyword': "str;list",
                'action': "str",
                'modmail': "str",
                'comment': "str",
                'message': "str",
                'report_reason': "str",
                'distinguish': "bool",
                'exempt_link_posts': "bool",
                'exempt_self_posts': "bool",
                'title_exempt_keyword': "str;list",
                'grace_period_mins': "int",
                'min_post_interval_hrs': "int",
                'min_post_interval_mins': "int",
                'approve': "bool",
                'lock_thread': "bool",
                'comment_stickied': "bool",
                'exempt_moderator_posts': "bool",
                'exempt_oc': "bool",
                'title_not_exempt_keyword': "str",
                'blacklist_enabled': "bool",

            }
            if not pr_settings:
                return False, "Bad config"
            for pr_setting in pr_settings:
                if pr_setting in possible_settings:
                    pr_setting_value = pr_settings[pr_setting]
                    pr_setting_value = True if pr_setting_value == 'True' else pr_setting_value
                    pr_setting_value = False if pr_setting_value == 'False' else pr_setting_value

                    pr_setting_type = type(pr_setting_value).__name__
                    # if possible_settings[pr_setting] not in f"{type(pr_settings[pr_setting])}":  will not work for true for modmail stting to use default template
                    #if "min" in pr_setting or "hrs" in pr_setting\
                    #        and isinstance(pr_settings[pr_setting], str):

                    # print(f"{self.subreddit_name}: {pr_setting} {pr_setting_value} {pr_setting_type}, {possible_settings[pr_setting]}")
                    if pr_setting_type == "NoneType" or pr_setting_type in possible_settings[pr_setting].split(";"):
                        setattr(self, pr_setting, pr_setting_value)

                    else:
                        return_text = f"{self.subreddit_name} invalid data type in yaml: `{pr_setting}` which " \
                                      f"is written as `{pr_setting_value}` should be of type " \
                                      f"{possible_settings[pr_setting]} but is type {pr_setting_type}"
                        print(return_text)
                        return False, return_text
                else:
                    return_text = "Did not understand variable '{}' for {}".format(pr_setting, self.subreddit_name)
                    print(return_text)

            if 'min_post_interval_mins' in pr_settings:
                self.min_post_interval = timedelta(minutes=pr_settings['min_post_interval_mins'])
                self.min_post_interval_txt = f"{pr_settings['min_post_interval_mins']}m"
            if 'min_post_interval_hrs' in pr_settings:
                self.min_post_interval = timedelta(hours=pr_settings['min_post_interval_hrs'])
                if self.min_post_interval_hrs < 24:
                    self.min_post_interval_txt = f"{self.min_post_interval_hrs}h"
                else:
                    self.min_post_interval_txt = f"{int(self.min_post_interval_hrs / 24)}d" \
                                                 f"{self.min_post_interval_hrs % 24}h".replace("d0h", "d")
            if 'grace_period_mins' in pr_settings and pr_settings['grace_period_mins'] is not None:
                self.grace_period = timedelta(minutes=pr_settings['grace_period_mins'])
                # self.grace_period_mins = pr_settings['grace_period_mins']
            if not self.ban_threshold_count:
                self.ban_threshold_count = 5

        if 'modmail' in self.settings_yaml:
            m_settings = self.settings_yaml['modmail']
            possible_settings = ('modmail_no_posts_reply', 'modmail_no_posts_reply_internal', 'modmail_posts_reply',
                                 'modmail_auto_approve_messages_with_links', 'modmail_all_reply',
                                 'modmail_notify_replied_internal', 'modmail_no_link_reply', 'canned_responses',
                                 'modmail_removal_reason_helper')
            if m_settings:
                for m_setting in m_settings:
                    if m_setting in possible_settings:
                        setattr(self, m_setting, m_settings[m_setting])
                    else:
                        return_text = "Did not understand variable '{}'".format(m_setting)

        self.min_post_interval = self.min_post_interval if self.min_post_interval else timedelta(hours=72)
        self.max_count_per_interval = self.max_count_per_interval if self.max_count_per_interval else 1
        self.last_updated = datetime.now()

        if self.ban_duration_days == 0:
            return False, "ban_duration_days can no longer be zero. Use `ban_duration_days: ~` to disable or use " \
                          "`ban_duration_days: 999` for permanent bans. Make sure there is a space after the colon."

        return True, return_text

    @staticmethod
    def get_subreddit_by_name(subreddit_name: str, create_if_not_exist=True):
        if subreddit_name.startswith("/r/"):
            subreddit_name = subreddit_name.replace('/r/', '')
        subreddit_name: str = subreddit_name.lower()
        tr_sub: TrackedSubreddit = s.query(TrackedSubreddit).get(subreddit_name)
        if not tr_sub:  # does not exist in database
            if not create_if_not_exist:
                return None
            try:
                tr_sub = TrackedSubreddit(subreddit_name)
                return tr_sub
            except prawcore.PrawcoreException:
                return None
        else:
            successful, message = tr_sub.update_from_yaml(force_update=False)  # load variables from stored yaml
        return tr_sub

    def get_author_summary(self, author_name: str) -> str:
        if author_name.startswith('u/'):
            author_name = author_name.replace("u/", "")

        recent_posts = s.query(SubmittedPost).filter(
            SubmittedPost.subreddit_name.ilike(self.subreddit_name),
            SubmittedPost.author == author_name,
            SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(days=182)).all()
        if not recent_posts:
            return "No posts found for {0} in {1}.".format(author_name, self.subreddit_name)
        diff = 0
        diff_str = "--"
        response_lines = [
            "For the last 4 months (since following this subreddit):\n\n|Time|Since Last|Author|Title|Status|\n"
            "|:-------|:-------|:------|:-----------|:------|\n"]
        for post in recent_posts:
            if diff != 0:
                diff_str = str(post.time_utc - diff)
            response_lines.append(
                "|{}|{}|u/{}|[{}]({})|{}|\n".format(post.time_utc, diff_str, post.author, post.title[0:15],
                                                    post.get_comments_url(),
                                                    post.get_posted_status().value))
            diff = post.time_utc
        response_lines.append(f"Current settings: {self.max_count_per_interval} post(s) "
                              f"per {self.min_post_interval_txt}")
        return "".join(response_lines)

    def get_sub_stats(self) -> str:
        total_reviewed = s.query(SubmittedPost) \
            .filter(SubmittedPost.subreddit_name.ilike(self.subreddit_name)) \
            .count()
        total_identified = s.query(SubmittedPost) \
            .filter(SubmittedPost.subreddit_name.ilike(self.subreddit_name)) \
            .filter(SubmittedPost.flagged_duplicate.is_(True)) \
            .count()

        authors = s.query(SubmittedPost, func.count(SubmittedPost.author).label('qty')) \
            .filter(SubmittedPost.subreddit_name.ilike(self.subreddit_name)) \
            .group_by(SubmittedPost.author).order_by(desc('qty')).limit(10).all().scalar()

        response_lines = ["Stats report for {0} \n\n".format(self.subreddit_name),
                          '|Author|Count|\n\n'
                          '|-----|----|']
        for post, count in authors:
            response_lines.append("|{}|{}|".format(post.author, count))

        return "total_reviewed: {}\n\n" \
               "total_identified: {}" \
               "\n\n{}".format(total_reviewed, total_identified, "\n\n".join(response_lines))

    def send_modmail(self, subject=f"[Notification] Message from {BOT_NAME}", body="Unspecfied text", thread_id=None):
        if thread_id:
            REDDIT_CLIENT.subreddit(self.subreddit_name).modmail(thread_id).reply(body, internal=true)
        else:
            try:
                REDDIT_CLIENT.subreddit(self.subreddit_name).message(subject, body)
            except (praw.exceptions.APIException, prawcore.exceptions.Forbidden, AttributeError):
                logger.warning('something went wrong in sending modmail')

    def populate_tags(self, input_text, recent_post=None, prev_post=None, post_list=None):
        if not isinstance(input_text, str):
            print("error: {0} is not a string".format(input_text))
            return "error: `{0}` is not a string in your config".format(str(input_text))
        if post_list and not prev_post:
            prev_post = post_list[0]
        if post_list and "{summary table}" in input_text:
            response_lines = ["\n\n|ID|Time|Author|Title|Status|Counted?|\n"
                              "|:---|:-------|:------|:-----------|:------|:------|\n"]
            for post in post_list:
                response_lines.append(
                    f"|{post.id}"
                    f"|{post.time_utc}"
                    f"|[{post.author}](/u/{post.author})"
                    f"|[{post.title}]({post.get_comments_url()})"
                    f"|{post.get_posted_status().value}"
                    f"|{CountedStatus(post.counted_status)}"
                    f"|\n")
            final_response = "".join(response_lines)
            input_text = input_text.replace("{summary table}", final_response)

        if prev_post:
            input_text = input_text.replace("{prev.title}", prev_post.title)
            if prev_post.submission_text:
                input_text = input_text.replace("{prev.selftext}", prev_post.submission_text)
            input_text = input_text.replace("{prev.url}", prev_post.get_url())
            input_text = input_text.replace("{time}", prev_post.time_utc.strftime("%Y-%m-%d %H:%M:%S UTC"))
            input_text = input_text.replace("{timedelta}", humanize.naturaltime(
                datetime.now(pytz.utc) - prev_post.time_utc.replace(tzinfo=timezone.utc)))
        if recent_post:
            input_text = input_text.replace("{author}", recent_post.author)
            input_text = input_text.replace("{title}", recent_post.title)
            input_text = input_text.replace("{url}", recent_post.get_url())


        input_text = input_text.replace("{subreddit}", self.subreddit_name)
        input_text = input_text.replace("{maxcount}", "{0}".format(self.max_count_per_interval))
        input_text = input_text.replace("{interval}", "{0}m".format(self.min_post_interval_txt))
        return input_text

    def populate_tags2(self, input_text, recent_post=None, prev_post=None, post_list=None):
        if not isinstance(input_text, str):
            print("error: {0} is not a string".format(input_text))
            return "error: `{0}` is not a string in your config".format(str(input_text))

        mydict = {"{subreddit}": self.subreddit_name, "{maxcount}": f"{self.max_count_per_interval}",
                  "{interval}": self.min_post_interval_txt}
        if recent_post:
            mydict.update({"{author}": recent_post.author, "{title}": recent_post.title,
                           "{url}": recent_post.get_url()})
        if post_list and not prev_post:
            prev_post = post_list[0]
        if post_list and "{summary table}" in input_text:
            response_lines = ["\n\n|ID|Time|Author|Title|Status|Counted?|\n"
                              "|:---|:-------|:------|:-----------|:------|:------|\n"]
            for post in post_list:
                response_lines.append(
                    f"|{post.id}"
                    f"|{post.time_utc}"
                    f"|[{post.author}](/u/{post.author})"
                    f"|[{post.title}]({post.get_comments_url()})"
                    f"|{post.get_posted_status().value}"
                    f"|{CountedStatus(post.counted_status)}"
                    f"|\n")
            final_response = "".join(response_lines)
            input_text = input_text.replace("{summary table}", final_response)

        if prev_post:
            if prev_post.submission_text:
                mydict["{prev.selftext}"] = prev_post.submission_text
            mydict.update({"{prev.title}": prev_post.title,"{prev.url}": prev_post.get_url(),
                           "{time}": prev_post.time_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                           "{timedelta}": humanize.naturaltime(datetime.now(pytz.utc)
                                                               - prev_post.time_utc.replace(tzinfo=timezone.utc)),
                           })


        input_text = re.sub(r'{(.+?)}', lambda m: mydict.get(m.group(), m.group()), input_text)
        return input_text

    def get_api_handle(self):
        if not self.api_handle:
            self.api_handle = REDDIT_CLIENT.subreddit(self.subreddit_name)
            return self.api_handle
        else:
            return self.api_handle



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

def check_for_post_exemptions(tr_sub: TrackedSubreddit, recent_post: SubmittedPost):
    # check if removed

    status = recent_post.get_posted_status(get_removed_info=True)
    # banned_by = recent_post.get_api_handle().banned_by
    # logger.debug(">>>>exemption status: {}".format(banned_by))

    if status == PostedStatus.SPAM_FLT:
        return CountedStatus.SPAMMED_EXMPT, ""
    elif tr_sub.ignore_AutoModerator_removed and status == PostedStatus.AUTOMOD_RM:
        return CountedStatus.AM_RM_EXEMPT, ""
    elif tr_sub.ignore_moderator_removed and status == PostedStatus.FH_RM:
        return CountedStatus.FLAIR_HELPER, ""
    elif tr_sub.ignore_moderator_removed and status == PostedStatus.MOD_RM:
        return CountedStatus.MOD_RM_EXEMPT, ""
    elif tr_sub.exempt_oc and recent_post.get_api_handle().is_original_content:
        return CountedStatus.OC_EXEMPT, ""
    elif tr_sub.exempt_self_posts and recent_post.get_api_handle().is_self:
        return CountedStatus.SELF_EXEMPT, ""
    elif tr_sub.exempt_link_posts and recent_post.get_api_handle().is_self is not True:
        return CountedStatus.LINK_EXEMPT, ""
    if tr_sub.exempt_moderator_posts and recent_post.author in tr_sub.subreddit_mods:
        return CountedStatus.MODPOST_EXEMPT, "moderator exempt"
    # check if flair-exempt
    author_flair = recent_post.get_api_handle().author_flair_text
    # add CSS class to author_flair
    if author_flair and recent_post.get_api_handle().author_flair_css_class:
        author_flair = author_flair + recent_post.get_api_handle().author_flair_css_class

    # Flair keyword exempt
    if tr_sub.author_exempt_flair_keyword and isinstance(tr_sub.author_exempt_flair_keyword, str) \
            and author_flair and tr_sub.author_exempt_flair_keyword in author_flair:
        logger.debug(">>>flair exempt")
        return CountedStatus.FLAIR_EXEMPT, "flair exempt {}".format(author_flair)

    # Not-flair-exempt keyword (Only restrict certain flairs)
    if tr_sub.author_not_exempt_flair_keyword \
            and ((author_flair and tr_sub.author_not_exempt_flair_keyword not in author_flair) or not author_flair):
        return CountedStatus.FLAIR_NOT_EXEMPT, "flair not exempt {}".format(author_flair)

    # check if title keyword exempt:
    if tr_sub.title_exempt_keyword:
        flex_title = recent_post.title.lower()
        if (isinstance(tr_sub.title_exempt_keyword, str)
            and tr_sub.title_exempt_keyword.lower() in flex_title) or \
                (isinstance(tr_sub.title_exempt_keyword, list)
                 and any(x in flex_title for x in [y.lower() for y in tr_sub.title_exempt_keyword])):
            logger.debug(">>>title keyword exempted")
            return CountedStatus.TITLE_KW_EXEMPT, f"title keyword exempt {tr_sub.title_exempt_keyword} -> exemption"

    # title keywords only to restrict:
    if tr_sub.title_not_exempt_keyword:
        linkflair = recent_post.get_api_handle().link_flair_text
        flex_title = recent_post.title.lower()
        if linkflair:
            flex_title = recent_post.title.lower() + linkflair

        if (isinstance(tr_sub.title_not_exempt_keyword, str)
            and tr_sub.title_not_exempt_keyword.lower() not in flex_title) or \
                (isinstance(tr_sub.title_not_exempt_keyword, list)
                 and all(x not in flex_title for x in [y.lower() for y in tr_sub.title_not_exempt_keyword])):
            logger.debug(">>>title keyword restricted")
            return CountedStatus.TITLE_KW_NOT_EXEMPT, f"title does not have {tr_sub.title_not_exempt_keyword} -> exemption"
    return CountedStatus.COUNTS, "no exemptions"

class PostingGroup:
    def __init__(self, author_name=None, subreddit_name=None, posts=None):
        self.author_name=author_name
        self.subreddit_name = subreddit_name
        self.posts = posts

def look_for_rule_violations2(do_cleanup: bool = False):
    global REDDIT_CLIENT
    global WATCHED_SUBS
    logger.debug("querying recent post(s)")

    faster_statement = "select max(t.id), group_concat(t.id order by t.id), group_concat(t.reviewed order by t.id), t.author, t.subreddit_name, count(t.author), max( t.time_utc), t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60 from RedditPost t inner join TrackedSubs s on t.subreddit_name = s.subreddit_name where counted_status <2 and t.time_utc> utc_timestamp() - Interval s.min_post_interval_mins  minute and t.time_utc > utc_timestamp() - Interval 72 hour group by t.author, t.subreddit_name having count(t.author) > s.max_count_per_interval and (max(t.time_utc)> max(t.last_checked) or max(t.last_checked) is NULL) order by max(t.time_utc) desc ;"

    more_accurate_statement = "SELECT MAX(t.id), GROUP_CONCAT(t.id ORDER BY t.id), GROUP_CONCAT(t.reviewed ORDER BY t.id), t.author, t.subreddit_name, COUNT(t.author), MAX(t.time_utc) as most_recent, t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60 FROM RedditPost t INNER JOIN TrackedSubs s ON t.subreddit_name = s.subreddit_name WHERE counted_status <2 AND t.time_utc > utc_timestamp() - INTERVAL s.min_post_interval_mins MINUTE  GROUP BY t.author, t.subreddit_name HAVING COUNT(t.author) > s.max_count_per_interval AND most_recent > utc_timestamp() - INTERVAL 72 HOUR AND (most_recent > MAX(t.last_checked) or max(t.last_checked) is NULL) ORDER BY most_recent desc ;"

    tick = datetime.now()
    if do_cleanup:
        print("doing more accurate")
        rs = s.execute(more_accurate_statement)
    else:
        print("doing usual")
        rs = s.execute(faster_statement)
    print(f"query took this long {datetime.now() - tick}")


    posting_groups=[]
    for row in rs:
        print(row[0], row[1], row[2], row[3], row[4])
        post_ids = row[1].split(',')
        posts = []
        for post_id in post_ids:
            # print(f"\t{post_id}")
            posts.append(s.query(SubmittedPost).get(post_id))
        # print(row[0], row[1], row[2], row[3], row[4])
        # post = s.query(SubmittedPost).get(row[0])
        # predecessors = row[1].split(',')
        # predecessors_times = row[2].split(',')
        posting_groups.append(PostingGroup(author_name=row[3], subreddit_name=row[4].lower(), posts=posts))
    print(f"Total found: {len(posting_groups)}")
    tick = datetime.now(pytz.utc)

    # Go through posting group
    for i, pg in enumerate(posting_groups):
        print(f"========================{i+1}/{len(posting_groups)}=================================")


        # Break if taking too long
        tock = datetime.now(pytz.utc) - tick
        if tock > timedelta(minutes=3) and do_cleanup is False:
            logger.debug("Aborting, taking more than 3 min")
            s.commit()
            break

        # Load subreddit settings
        tr_sub = update_list_with_subreddit(pg.subreddit_name, request_update_if_needed=True)
        max_count = tr_sub.max_count_per_interval

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
                post.update_status(reviewed=True, flagged_duplicate=True, counted_status=CountedStatus.BLKLIST)
                s.add(post)
                logger.info(
                    f"{i}-{j}\t\tpost removed - prior to eligibility")
                try:
                    success = post.mod_remove()  # no checking if can't remove post
                    if success and tr_sub.comment:
                        last_valid_post: SubmittedPost = s.query(SubmittedPost).get(
                            subreddit_author.last_valid_post) if subreddit_author.last_valid_post is not None else None
                        make_comment(tr_sub, post, [last_valid_post, ],
                                     tr_sub.comment, distinguish=tr_sub.distinguish, approve=tr_sub.approve,
                                     lock_thread=tr_sub.lock_thread, stickied=tr_sub.comment_stickied,
                                     next_eligibility=subreddit_author.next_eligible, blacklist=True)
                except (praw.exceptions.APIException, prawcore.exceptions.Forbidden) as e:
                    logger.warning('something went wrong in removing post %s', str(e))
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
        #send_modmail_populate_tags(tr_sub, message, recent_post=recent_post, prev_post=possible_repost, )
        tr_sub.send_modmail(body=tr_sub.populate_tags(message, recent_post=recent_post, prev_post=possible_repost),
                            subject="[Notification] Post that violates rule frequency restriction")
    if tr_sub.action == "remove":
        post_status = recent_post.get_posted_status()
        if post_status == PostedStatus.UP:
            try:
                was_successful = recent_post.mod_remove()
                logger.debug("\tremoved post now")
                if not was_successful:
                    logger.debug("\tcould not remove post")
            except praw.exceptions.APIException:
                logger.debug("\tcould not remove post")
            except prawcore.exceptions.Forbidden:
                logger.debug("\tcould not remove post: Forbidden")
        else:
            logger.debug("\tpost not up")

    if tr_sub.action == "report":
        if tr_sub.report_reason:
            rp_reason = tr_sub.populate_tags(tr_sub.report_reason, recent_post=recent_post, prev_post=possible_repost)
            recent_post.get_api_handle().report(("ModeratelyHelpfulBot:" + rp_reason)[0:99])
        else:
            recent_post.get_api_handle().report("ModeratelyHelpfulBot: repeatedly exceeding posting threshold")
    if tr_sub.message and recent_post.author:
        recent_post.get_api_handle().author.message("Regarding your post",
                                                    tr_sub.populate_tags(tr_sub.message, recent_post=recent_post,
                                                                         post_list=most_recent_reposts))


def check_for_actionable_violations(tr_sub: TrackedSubreddit, recent_post: SubmittedPost,
                                    most_recent_reposts: List[SubmittedPost]):
    possible_repost = most_recent_reposts[-1]
    tick = datetime.now(pytz.utc)
    other_spam_by_author = s.query(SubmittedPost).filter(
        # SubmittedPost.flagged_duplicate.is_(True),
        SubmittedPost.counted_status == CountedStatus.FLAGGED.value,
        SubmittedPost.author == recent_post.author,
        SubmittedPost.subreddit_name.ilike(tr_sub.subreddit_name),
        SubmittedPost.time_utc < recent_post.time_utc) \
        .all()

    logger.info("Author {0} had {1} rule violations. Banning if at least {2} - query time took: {3}"
                .format(recent_post.author, len(other_spam_by_author), tr_sub.ban_threshold_count,
                        datetime.now(pytz.utc) - tick))

    if tr_sub.ban_duration_days is None or isinstance(tr_sub.ban_duration_days, str):
        logger.info("No bans per wiki. ban_duration_days is {}".format(tr_sub.ban_duration_days))
        if tr_sub.ban_ability != 0:
            tr_sub.ban_ability = 0
            s.add(tr_sub)
            s.commit()
        # if len(most_recent_reposts) > 2:  this doesn't work - doesn't coun't bans
        #    logger.info("Adding to soft blacklist based on next eligibility - for tracking only")
        #    next_eligibility = most_recent_reposts[0].time_utc + subreddit.min_post_interval
        #    soft_blacklist(tr_sub, recent_post, next_eligibility)
        return

    if len(other_spam_by_author) == tr_sub.ban_threshold_count - 1 and tr_sub.ban_threshold_count > 1:
        try:
            # tr_sub.ignore_AutoModerator_removed

            REDDIT_CLIENT.redditor(recent_post.author).message(
                f"Beep! Boop! Please note that you are close approaching "
                f"your posting limit for {recent_post.subreddit_name}",
                f"This subreddit (/r/{recent_post.subreddit_name}) only allows {tr_sub.max_count_per_interval} post(s) "
                f"per {humanize.precisedelta(tr_sub.min_post_interval)}. "
                f"This {'does NOT' if tr_sub.ignore_moderator_removed else 'DOES'} include mod-removed posts. "
                f"While this post was within the post limiting rule and not removed by this bot, "
                f"please do not make any new posts before "
                f"{most_recent_reposts[0].time_utc + tr_sub.min_post_interval} UTC, as it "
                f"may result in a ban. If you made a title mistake you have "
                f"STRICTLY {humanize.precisedelta(tr_sub.grace_period)} to delete it and repost it. "
                f"This is an automated message. "
            )
        except praw.exceptions.APIException:
            pass

    if len(other_spam_by_author) >= tr_sub.ban_threshold_count:
        num_days = tr_sub.ban_duration_days

        if 0 < num_days < 1:
            num_days = 1
        if num_days > 998:
            num_days = 999
        if num_days == 0:
            num_days = 999

        str_prev_posts = ",".join(
            [" [{0}]({1})".format(a.id, "http://redd.it/{}".format(a.id)) for a in other_spam_by_author])

        ban_message = f"This subreddit (/r/{recent_post.subreddit_name}) only allows {tr_sub.max_count_per_interval} " \
                      f"post(s) per {humanize.precisedelta(tr_sub.min_post_interval)}, and it only allows for " \
                      f"{tr_sub.ban_threshold_count} violation(s) of this rule. This is a rolling limit and " \
                      f"includes self-deletions. Per our records, there were {len(other_spam_by_author)} post(s) " \
                      f"from you that went beyond the limit: {str_prev_posts} If you think you may have been hacked, " \
                      f"please change your passwords NOW. "
        time_next_eligible = datetime.now(pytz.utc) + timedelta(days=num_days)

        # If banning is specified but not enabled, just go to blacklist. Don't bother trying to ban without access.
        if tr_sub.ban_ability == -2:
            if tr_sub.ban_duration_days > 998:
                # Only do a 2 week ban if specified permanent ban
                time_next_eligible = datetime.now(pytz.utc) + timedelta(days=999)
            elif tr_sub.ban_duration_days == 0:
                # Only do a 2 week ban if specified permanent ban
                time_next_eligible = datetime.now(pytz.utc) + timedelta(days=14)

            soft_blacklist(tr_sub, recent_post, time_next_eligible)
            return

        try:
            if num_days == 999:
                # Permanent ban
                REDDIT_CLIENT.subreddit(tr_sub.subreddit_name).banned.add(
                    recent_post.author, ban_note="ModhelpfulBot: repeated spam", ban_message=ban_message[:999])
                logger.info(f"PERMANENT ban for {recent_post.author} succeeded ")
            else:
                # Not permanent ban
                ban_message += f"\n\nYour ban will last {num_days} day{'s' if num_days>1 else ''} from this message. " \
                               f"**Repeat infractions result in a permanent ban!**"

                REDDIT_CLIENT.subreddit(tr_sub.subreddit_name).banned.add(
                            recent_post.author, ban_note="ModhelpfulBot: repeated spam", ban_message=ban_message[:999],
                            duration=num_days)
                logger.info(f"Ban for {recent_post.author} succeeded for {num_days} days")
        except praw.exceptions.APIException:
            pass
        except prawcore.exceptions.Forbidden:

            logger.info("Ban failed - no access?")
            tr_sub.ban_ability = -2
            if tr_sub.notify_about_spammers:
                response_lines = [
                    "This person has multiple rule violations. "
                    "Please adjust my privileges and ban threshold "
                    "if you would like me to automatically ban them.\n\n".format(
                        recent_post.author, len(other_spam_by_author), tr_sub.ban_threshold_count)]

                for post in other_spam_by_author:
                    response_lines.append(f"* {post.time_utc}: "
                                          f"[{post.author}](/u/{post.author}) "
                                          f"[{post.title}]({post.get_comments_url()})\n")
                response_lines.append(f"* {recent_post.time_utc}: "
                                      f"[{recent_post.author}](/u/{recent_post.author}) "
                                      f"[{recent_post.title}]({recent_post.get_comments_url()})\n")

                # send_modmail_populate_tags(tr_sub, "\n\n".join(response_lines), recent_post=recent_post, prev_post=possible_repost)
                tr_sub.send_modmail(subject="[Notification] Multiple post frequency violations",
                                    body=tr_sub.populate_tags2("\n\n".join(response_lines),
                                                              recent_post=recent_post, prev_post=possible_repost))
            if tr_sub.ban_duration_days > 998:
                # Only do a 2 week ban if specified permanent ban
                time_next_eligible = datetime.now(pytz.utc) + timedelta(days=999)
            elif tr_sub.ban_duration_days == 0:
                # Only do a 2 week ban if specified permanent ban
                time_next_eligible = datetime.now(pytz.utc) + timedelta(days=14)

            soft_blacklist(tr_sub, recent_post, time_next_eligible)


def soft_blacklist(tr_sub: TrackedSubreddit, recent_post: SubmittedPost, time_next_eligible: datetime):
    # time_next_eligible = datetime.now(pytz.utc) + timedelta(days=num_days)
    logger.info("Author added to blacklisted 2/2 no permission to ban. Ban duration is {}"
                .format(tr_sub.ban_duration_days, ))
    # Add to the watch list
    subreddit_author: SubAuthor = s.query(SubAuthor).get((tr_sub.subreddit_name, recent_post.author))
    if not subreddit_author:
        subreddit_author = SubAuthor(tr_sub.subreddit_name, recent_post.author)
    subreddit_author.last_valid_post = recent_post.id
    subreddit_author.next_eligible = time_next_eligible
    s.add(subreddit_author)
    s.add(tr_sub)
    s.commit()




def make_comment(subreddit: TrackedSubreddit, recent_post: SubmittedPost, most_recent_reposts, comment_template: String,
                 distinguish=False, approve=False, lock_thread=True, stickied=False, next_eligibility: datetime = None,
                 blacklist=False):
    prev_submission = most_recent_reposts[-1] if most_recent_reposts else None
    if not next_eligibility:
        next_eligibility = most_recent_reposts[0].time_utc + subreddit.min_post_interval
    # print(most_recent_reposts)
    reposts_str = ",".join(
        [f" [{a.id}]({a.get_comments_url()})" for a in most_recent_reposts]) \
        if most_recent_reposts and most_recent_reposts[0] else "BL"
    if blacklist:
        reposts_str = " Temporary lock out per" + reposts_str
    else:
        reposts_str = " Previous post(s):" + reposts_str
    ids = f"{reposts_str} | limit: {{maxcount}} per {{interval}} | " \
          f"next eligibility: {next_eligibility.strftime('%Y-%m-%d %H:%M UTC')}"

    ids = ids.replace(" ", " ^^")
    comment = None
    response = subreddit.populate_tags2(f"{comment_template}{RESPONSE_TAIL}{ids}",
                                       recent_post=recent_post, prev_post=prev_submission)
    try:
        comment: praw.models.Comment = \
            recent_post.reply(response, distinguish=distinguish, approve=approve, lock_thread=lock_thread)
        # assert comment
        if stickied:
            comment.mod.distinguish(sticky=True)
        try:
            recent_post.bot_comment_id = comment.id
        except (AttributeError):
            pass
    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden) as e:
        logger.warning('something went wrong in creating comment %s', str(e))
    return comment


def load_settings():
    global RESPONSE_TAIL
    global MAIN_SETTINGS
    wiki_settings = REDDIT_CLIENT.subreddit('moderatelyhelpfulbot').wiki['moderatelyhelpfulbot']
    MAIN_SETTINGS = yaml.safe_load(wiki_settings.content_md)

    if 'response_tail' in MAIN_SETTINGS:
        RESPONSE_TAIL = MAIN_SETTINGS['response_tail']
    # load_subs(main_settings)


def check_actioned(comment_id: str):
    response: ActionedComments = s.query(ActionedComments).get(comment_id)
    if response:
        return True
    return False


def record_actioned(comment_id: str):
    response: ActionedComments = s.query(ActionedComments).get(comment_id)
    if response:
        return
    s.add(ActionedComments(comment_id))
    # s.commit()


def send_broadcast_messages():
    global WATCHED_SUBS
    broadcasts = s.query(Broadcast) \
        .filter(Broadcast.sent.is_(False)) \
        .all()
    if broadcasts:
        update_list_with_all_active_subs()
    try:
        for broadcast in broadcasts:
            if broadcast.subreddit_name == "all":
                for subreddit_name in WATCHED_SUBS:
                    REDDIT_CLIENT.subreddit(subreddit_name).message(broadcast.title, broadcast.text)
            else:
                REDDIT_CLIENT.subreddit(broadcast.subreddit_name).message(broadcast.title, broadcast.text)
            broadcast.sent = True
            s.add(broadcast)

    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
        logger.warning('something went wrong in sending broadcast modmail')
    s.commit()


def handle_dm_command(subreddit_name: str, requestor_name, command, parameters, thread_id=None) -> (str, bool):
    subreddit_name: str = subreddit_name[2:] if subreddit_name.startswith('r/') else subreddit_name
    subreddit_name: str = subreddit_name[3:] if subreddit_name.startswith('/r/') else subreddit_name
    command: str = command[1:] if command.startswith("$") else command

    tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name, create_if_not_exist=True)
    if not tr_sub:
        return "Error retrieving information for /r/{}".format(subreddit_name), True
    moderators: List[str] = tr_sub.get_mods_list()
    print("asking for permission: {}, mod list: {}".format(requestor_name, ",".join(moderators)))
    if requestor_name is not None and requestor_name not in moderators and requestor_name != BOT_OWNER \
            and requestor_name != "[modmail]":
        if subreddit_name is "subredditname":
            return "Please change 'subredditname' to the name of your subreddit so I know what subreddit you mean!", \
                   True
        return f"You do not have permission to do this. Are you sure you are a moderator of {subreddit_name}?\n\n " \
               f"/r/{subreddit_name} moderator list: {str(moderators)}, your name: {requestor_name}", True

    if tr_sub.bot_mod is None and requestor_name in moderators:
        tr_sub.bot_mod = requestor_name
        s.add(tr_sub)
        s.commit()

    if command == "summary":
        author_name_to_check = parameters[0] if parameters else None
        if not author_name_to_check:
            return "No author name given", True
        return tr_sub.get_author_summary(author_name_to_check), True
    elif command == "showrules":
        lines = ["Rules for {}:".format(subreddit_name), ]
        rules = REDDIT_CLIENT.subreddit(subreddit_name).rules()['rules']
        for count, rule in enumerate(rules):
            lines.append("{}: {}".format(count + 1, rule['short_name']))
        return "\n\n".join(lines), True
    elif command == "stats":
        return tr_sub.get_sub_stats(), True
    elif command == "approve":
        submission_id = parameters[0] if parameters else None
        if not submission_id:
            return "No submission name given", True
        submission = REDDIT_CLIENT.submission(submission_id)
        if not submission:
            return "Cannot find that submission", True
        submission.mod.approve()
        return "Submission was approved.", False

    elif command == "remove":
        submission_id = parameters[0] if parameters else None
        if not submission_id:
            return "No author name given", True
        submission = REDDIT_CLIENT.submission(submission_id)
        if not submission:
            return "Cannot find that submission", True
        submission.mod.remove()
        return "Submission was removed.", True
    elif command == "hallpass":
        author_name_to_check: str = parameters[0] if parameters else None
        if not author_name_to_check:
            return "No author name given", True
        if author_name_to_check.startswith('u/'):
            author_name_to_check = author_name_to_check.replace("u/", "")
        author_name_to_check = author_name_to_check.lower()
        actual_author = REDDIT_CLIENT.redditor(author_name_to_check)
        _ = actual_author.id  # force load actual username capitalization
        if not actual_author:
            return "could not find that username `{}`".format(author_name_to_check), True

        subreddit_author: SubAuthor = s.query(SubAuthor).get((subreddit_name, actual_author.name))
        if not subreddit_author:
            subreddit_author = SubAuthor(tr_sub.subreddit_name, actual_author.name)
        subreddit_author.hall_pass = 1
        s.add(subreddit_author)
        return_text = "User {} has been granted a hall pass. " \
                      "This means the next post by the user in this subreddit will not be automatically removed." \
            .format(actual_author.name)
        return return_text, False
    elif command == "blacklist":
        author_name_to_check = parameters[0] if parameters else None
        if not author_name_to_check:
            return "No author name given", True
        if author_name_to_check.startswith('u/'):
            author_name_to_check = author_name_to_check.replace("u/", "")
        author_name_to_check = author_name_to_check.lower()
        actual_author = REDDIT_CLIENT.redditor(author_name_to_check)
        _ = actual_author.id  # force load actual username capitalization
        if not actual_author:
            return "could not find that username `{}`".format(author_name_to_check), True

        subreddit_author: SubAuthor = s.query(SubAuthor).get((subreddit_name, actual_author.name))
        if not subreddit_author:
            subreddit_author = SubAuthor(tr_sub.subreddit_name, actual_author.name)
        subreddit_author.currently_blacklisted = True
        s.add(subreddit_author)
        return_text = "User {} has been blacklisted from modmail. " \
            .format(actual_author.name)
        return return_text, True
    elif command == "citerule" or command == "testciterule":
        if not parameters:
            return "No rule # given", True
        try:
            # converting to integer
            rule_num = int(parameters[0]) - 1
        except ValueError:
            return "invalid rule #`{}`".format(parameters[0]), True
        rules = REDDIT_CLIENT.subreddit(subreddit_name).rules()['rules']
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
        rules = REDDIT_CLIENT.subreddit(subreddit_name).rules()['rules']
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
        author_name_to_check = parameters[0] if parameters else None
        subreddit_author: SubAuthor = s.query(SubAuthor).get((tr_sub.subreddit_name, author_name_to_check))
        if subreddit_author and subreddit_author.next_eligible.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc):
            subreddit_author.next_eligible = datetime(2019, 1, 1, 0, 0)
            return "User was removed from blacklist", False
        posts = s.query(SubmittedPost).filter(SubmittedPost.author == author_name_to_check,
                                              # SubmittedPost.flagged_duplicate.is_(True),
                                              SubmittedPost.counted_status == CountedStatus.FLAGGED.value,
                                              SubmittedPost.subreddit_name == tr_sub.subreddit_name).all()
        for post in posts:
            post.flagged_duplicate = False
            post.counted_status = CountedStatus.EXEMPTED.value
            s.add(post)
        s.commit()
    elif command == "unban":
        author_name_to_check = parameters[0] if parameters else None
        subreddit_author: SubAuthor = s.query(SubAuthor).get((tr_sub.subreddit_name, author_name_to_check))
        if subreddit_author and subreddit_author.next_eligible.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc):
            subreddit_author.next_eligible = datetime(2019, 1, 1, 0, 0)
            return "User was removed from blacklist", False
        try:
            REDDIT_CLIENT.subreddit(tr_sub.subreddit_name).banned.remove(author_name_to_check)
            return "Unban succeeded", False
        except prawcore.exceptions.Forbidden:
            return "Unban failed, I don't have permission in this subreddit to do that. Sorry.", True
    elif command == "ban":
        author_name_to_check = parameters[0] if parameters else None
        author_name_to_check = author_name_to_check.replace('/u/', '')
        author_name_to_check = author_name_to_check.replace('u/', '')

        ban_length = int(parameters[1]) if parameters and len(parameters) >= 2 else None

        ban_reason = "per modmail command"
        if not author_name_to_check:
            return "No author name given", True
        actual_author = REDDIT_CLIENT.redditor(author_name_to_check)
        if not actual_author:
            return "Invalid author name or deleted account", True
        print('trying to ban: {}'.format(author_name_to_check))

        try:
            if ban_length:
                REDDIT_CLIENT.subreddit(tr_sub.subreddit_name).banned.add(
                    author_name_to_check, ban_note="ModhelpfulBot: per modmail command", ban_message=ban_reason,
                    ban_length=ban_length)
                return "Ban for {} was successful".format(author_name_to_check), True
            else:
                REDDIT_CLIENT.subreddit(tr_sub.subreddit_name).banned.add(
                    author_name_to_check, ban_note="ModhelpfulBot: per modmail command", ban_message=ban_reason)
                return "Ban for {} was successful".format(author_name_to_check), True
        except prawcore.exceptions.Forbidden:
            return "Ban failed, I don't have permission to do that", True

    elif command == "update":
        worked, status = tr_sub.update_from_yaml(force_update=True)
        help_text = ""
        if "404" in status:
            help_text = f"This error means the wiki config page needs to be created. " \
                        f" See https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/moderatelyhelpfulbot. "
        elif "403" in status:
            help_text = f"This error means the bot doesn't have enough permissions to view the wiki page. " \
                        f"Please make sure that the bot has accepted the moderator invitation and give the bot wiki " \
                        f"privileges here: https://www.reddit.com/r/{tr_sub.subreddit_name}/about/moderators/ . " \
                        f"It is possible that the bot has not accepted the invitation due to current load.  "
        elif "yaml" in status:
            help_text = "Looks like there is an error in your yaml code. " \
                        "Please make sure to validate your syntax at https://yamlvalidator.com/.  "
        elif "single document in the stream" in status:
            help_text = "Looks like there is an extra double hyphen in your code at the end, e.g. '--'. " \
                        "Please remove it.  "

        reply_text = "Received message to update config for {0}.  See the output below. {2}" \
                     "Please message [/r/moderatelyhelpfulbot](https://www.reddit.com/" \
                     "message/compose?to=%2Fr%2Fmoderatelyhelpfulbot) if you have any questions \n\n" \
                     "Update report: \n\n >{1}".format(subreddit_name, status, help_text)
        bot_owner_message = "subreddit: {0}\n\nrequestor: {1}\n\nreport: {2}" \
            .format(subreddit_name, requestor_name, status)
        # REDDIT_CLIENT.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
        BOT_SUB.send_modmail(body=bot_owner_message)
        s.add(tr_sub)
        s.commit()
        return reply_text, True
    else:
        return "I did not understand that command", True


def handle_direct_messages():
    # Reply to pms or
    global WATCHED_SUBS
    for message in REDDIT_CLIENT.inbox.unread(limit=None):
        logger.info("got this email author:{} subj:{}  body:{} ".format(message.author, message.subject, message.body))

        # Get author name, message_id if available
        requestor_name = message.author.name if message.author else None
        message_id = REDDIT_CLIENT.comment(message.id).link_id if message.was_comment else message.name
        body_parts = message.body.split(' ')
        command = body_parts[0].lower() if len(body_parts) > 0 else None
        # subreddit_name = message.subject.replace("re: ", "") if command else None
        # First check if already actioned
        if check_actioned(message_id):
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
            if not check_actioned("ban_note: {0}".format(requestor_name)):
                # record actioned first out of safety in case of error
                # TODO: write help message for other types of bans - or detect removal reason
                record_actioned("ban_note: {0}".format(message.author))

                tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
                if tr_sub and tr_sub.modmail_posts_reply and message.author:
                    try:
                        message.reply(tr_sub.get_author_summary(message.author.name))
                    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                        pass
        # Respond to an invitation to moderate
        elif message.subject.startswith('invitation to moderate'):
            mod_mail_invitation_to_moderate(message)
        elif command in ("summary", "update", "stats") or command.startswith("$"):
            subject_parts = message.subject.split(":")
            thread_id = subject_parts[1] if len(subject_parts)>1 else None
            subreddit_name = subject_parts[0].lower().replace("re: ", "")
            tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
            response, _ = handle_dm_command(subreddit_name, requestor_name, command, body_parts[1:])
            if tr_sub and thread_id:
                tr_sub.send_modmail(body=response[:9999], thread_id=thread_id)
            else:
                message.reply(response[:9999])
            bot_owner_message = "subreddit: {0}\n\nrequestor: {1}\n\nreport: {2}\n\nreport: {3}" \
                .format(subreddit_name, requestor_name, command, response)
            BOT_SUB.send_modmail(subject="[Notification]  Command processed", body=bot_owner_message)
            #REDDIT_CLIENT.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)

        elif requestor_name and not check_actioned(requestor_name):
            record_actioned(requestor_name)
            message.mark_read()
            try:
                # ignore profanity
                if "fuck" in message.body:
                    continue
                message.reply("Hi, thank you for messaging me! "
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
        record_actioned(message_id)
    s.commit()


def mod_mail_invitation_to_moderate(message):
    subreddit_name = message.subject.replace("invitation to moderate /r/", "")
    tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name, create_if_not_exist=False)

    # accept invite if accepting invites or had been accepted previously
    if ACCEPTING_NEW_SUBS or tr_sub:
        sub = REDDIT_CLIENT.subreddit(subreddit_name)
        try:
            sub.mod.accept_invite()
        except praw.exceptions.APIException:
            message.reply("Error: Invite message has been rescinded?")

        message.reply(f"Hi, thank you for inviting me!  I will start working now. Please make sure I have a config. "
                      f"It should be at https://www.reddit.com/r/{subreddit_name}/wiki/{BOT_NAME} . "
                      f"You may need to create it. You can find examples at "
                      f"https://www.reddit.com/r/{BOT_NAME}/wiki/index . ")
    else:
        message.reply(f"Invitation received. Please wait for approval by bot owner. In the mean time, "
                      f"you may create a config at https://www.reddit.com/r/{subreddit_name}/wiki/{BOT_NAME} .")
    message.mark_read()


def handle_modmail_message(convo):
    # Ignore old messages past 24h
    if iso8601.parse_date(convo.last_updated) < datetime.now(timezone.utc) - timedelta(hours=24):
        convo.read()

        return
    initiating_author_name = convo.authors[0].name
    subreddit_name = convo.owner.display_name
    if subreddit_name not in WATCHED_SUBS:
        update_list_with_subreddit(subreddit_name)
    tr_sub = WATCHED_SUBS[subreddit_name]
    if not tr_sub:
        return

    # Ignore if already actioned (at this many message #s)
    if check_actioned("mm{}-{}".format(convo.id, convo.num_messages)):
        try:
            convo.read()
        except prawcore.exceptions.Forbidden:
            pass

        return

    if initiating_author_name:
        subreddit_author: SubAuthor = s.query(SubAuthor).get((subreddit_name, initiating_author_name))
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
            record_actioned("mm{}-{}".format(convo.id, convo.num_messages))
            return

        import re
        submission = None
        urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
        if len(urls) == 2:  # both link and link description
            print(f"found url: {urls[0][1]}")
            submission = REDDIT_CLIENT.submission(urls[0][1])

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
            recent_posts: List[SubmittedPost] = s.query(SubmittedPost) \
                .filter(SubmittedPost.subreddit_name.ilike(subreddit_name)) \
                .filter(SubmittedPost.author == initiating_author_name).all()
            removal_reason = None
            # Check again if still no posts in database
            if not recent_posts:
                check_spam_submissions(subreddit_name)
                recent_posts: List[SubmittedPost] = s.query(SubmittedPost) \
                    .filter(SubmittedPost.subreddit_name.ilike(subreddit_name)) \
                    .filter(SubmittedPost.author == initiating_author_name).all()
            # Collect removal reason if possible from bot comment or mod comment
            # TODO check for report reason built into post
            last_post = None
            if submission:
                last_post = s.query(SubmittedPost).get(submission.id)
                if last_post:
                    last_post.api_handle = submission
            if recent_posts:
                last_post = recent_posts[-1] if not last_post else last_post
                # if removal reason hasn't been pulled, try pulling again
                if not last_post.bot_comment_id:
                    posted_status = last_post.get_posted_status(get_removed_info=True)
                    # update db if found an explanation
                    if last_post.bot_comment_id:  # found it now?
                        s.add(last_post)
                        s.commit()
                        removal_reason = last_post.get_removed_explanation()  # try to get removal reason
                        if removal_reason:
                            removal_reason = removal_reason.replace("\n\n", "\n\n>")
                            removal_reason = f"-------------------------------------------------\n\n{removal_reason}"
                    if not removal_reason:  # still couldn't find removal reason, just use posted status
                        removal_reason = f"status: {posted_status.value}\n\n " \
                                         f"flair: {last_post.get_api_handle().link_flair_text}"
                        if posted_status == PostedStatus.SPAM_FLT:
                            removal_reason += " \n\nThis means the Reddit spam filter thought your post was spam " \
                                              "and it was NOT removed by the subreddit moderators.  You can try" \
                                              "verifying your email and building up karma to avoid the spam filter." \
                                              "There is more information here: " \
                                              "https://www.reddit.com/r/NewToReddit/wiki/ntr-guidetoreddit"
                        r_last_post = last_post.get_api_handle()
                        if hasattr(r_last_post, 'removal_reason') and r_last_post.removal_reason \
                                and not posted_status == posted_status.UP:
                            removal_reason += f"\n\n{r_last_post.removal_reason}"

            # All reply if specified
            if not response and tr_sub.modmail_all_reply and tr_sub.modmail_all_reply is not True:
                #response = populate_tags(tr_sub.modmail_all_reply, None, tr_sub=tr_sub, prev_posts=recent_posts)
                response = tr_sub.populate_tags2(tr_sub.modmail_all_reply, post_list=recent_posts)
            # No links auto reply if specified
            if not response and tr_sub.modmail_no_link_reply:
                import re
                urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
                if len(urls) < 2:  # both link and link description
                    #response = populate_tags(tr_sub.modmail_no_link_reply, None, tr_sub=tr_sub, prev_posts=recent_posts)
                    response = tr_sub.populate_tags2(tr_sub.modmail_no_link_reply, post_list=recent_posts)
                    # Add last found link
                    if recent_posts:
                        response += f"\n\nAre you by chance referring to this post? {recent_posts[-1].get_comments_url()}"
                        # Add removal reason if found
                        if removal_reason:
                            response += f"\n\nIf so, does this answer your question?\n\n>{removal_reason}"
                    #debug_notify = True

            # Having previous posts reply
            if not response and recent_posts and tr_sub.modmail_posts_reply:
                # Does have recent posts -> reply with posts reply
                if tr_sub.modmail_posts_reply is True:  # default -> only goes to internal
                    response = ">" + convo.messages[0].body_markdown.replace("\n\n", "\n\n>")
                    if removal_reason and tr_sub.modmail_removal_reason_helper:
                        # response += "\n\n-------------------------------------------------"
                        # response += f"\n\nRemoval reason from [post]({last_post.get_comments_url()}):\n\n {removal_reason})"
                        # response += "\n\n-------------------------------------------------"
                        non_internal_response = f"AUTOMATED RESPONSE with reference information " \
                                                f"(please ignore unless relevant):\n\n " \
                                                f"last post: [{last_post.title}]({last_post.get_comments_url()})\n\n" \
                                                f"{removal_reason}"
                        if "status:mod-removed" not in non_internal_response\
                                and "status: AutoMod-removed" not in non_internal_response:
                            #don't answer if not particularly helpful
                            convo.reply(non_internal_response, internal=False)
                    smart_link = f"https://www.reddit.com/message/compose?to=ModeratelyHelpfulBot" \
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
                        f"\n\nPlease subscribe to /r/ModeratelyHelpfulBot for updates\n\n", None,
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
                response, response_internal = handle_dm_command(subreddit_name, last_author_name, command,
                                                                body_parts[1:])
            #Catch messages that weren't meant to be internal
            elif convo.num_messages > 2 and convo.messages[-2].author.name == BOT_NAME and last_message.is_internal:
                if not check_actioned(f"ic-{convo.id}") and tr_sub.modmail_notify_replied_internal:
                    response = "Hey sorry to bug you, but was this last message not meant to be moderator-only?  " \
                               "Just checking!  \n\n" \
                               "Set `modmail_notify_replied_internal` to False to disable this message"

                    response_internal = True
                    record_actioned(f"ic-{convo.id}")
    if response:
        try:
            convo.reply(tr_sub.populate_tags2(response[0:9999], recent_post=last_post), internal=response_internal)

            bot_owner_message = f"subreddit: {subreddit_name}\n\nresponse:\n\n{response}\n\n" \
                                f"https://mod.reddit.com/mail/all/{convo.id}"[0:9999]

            if debug_notify:
                # REDDIT_CLIENT.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
                mhb_sub = TrackedSubreddit.get_subreddit_by_name("ModeratelyHelpfulBot")
                mhb_sub.send_modmail(subject="[Notification] MHB Command used", body=bot_owner_message)
        except (prawcore.exceptions.BadRequest, praw.exceptions.RedditAPIException):
            logger.debug("reply failed {0}".format(response))
    record_actioned("mm{}-{}".format(convo.id, convo.num_messages))
    convo.read()
    s.commit()


def handle_modmail_messages():
    print("checking modmail")
    global WATCHED_SUBS

    for convo in REDDIT_CLIENT.subreddit('all').modmail.conversations(state="mod", sort='unread', limit=15):
        handle_modmail_message(convo)

    for convo in REDDIT_CLIENT.subreddit('all').modmail.conversations(state="join_requests", sort='unread', limit=15):
        handle_modmail_message(convo)

    for convo in REDDIT_CLIENT.subreddit('all').modmail.conversations(state="all", sort='unread', limit=15):
        handle_modmail_message(convo)

    # properties for message: body_markdown, author.name, id, is_internal, date
    # properties for convo: authors (list), messages,
    # mod_actions, num_messages, obj_ids, owner (subreddit obj), state, subject, user


def update_list_with_all_active_subs():
    global WATCHED_SUBS
    subs = s.query(TrackedSubreddit).filter(TrackedSubreddit.last_updated > datetime.now() - timedelta(days=3)).all()
    for sub in subs:
        if sub.subreddit_name not in WATCHED_SUBS:
            update_list_with_subreddit(sub.subreddit_name)


def update_list_with_subreddit(subreddit_name: str, request_update_if_needed=False):
    global WATCHED_SUBS
    to_update_db = False

    if subreddit_name in WATCHED_SUBS:  # check if loaded
        tr_sub = WATCHED_SUBS[subreddit_name]
    else:  # not loaded into memory
        tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name, create_if_not_exist=False)
        if not tr_sub:  # not in database -> create it
            tr_sub = TrackedSubreddit(subreddit_name)
            to_update_db = True
        WATCHED_SUBS[subreddit_name] = tr_sub  # store in memory

    # request updated if needed
    if request_update_if_needed and \
            tr_sub.last_updated < datetime.now() - timedelta(hours=SUBWIKI_CHECK_INTERVAL_HRS):
        worked, status = tr_sub.update_from_yaml(force_update=True)

        #  Notify (only once) if updating did not work.
        if not worked and hasattr(tr_sub, "settings_revision_date"):
            if not check_actioned(f"wu-{subreddit_name}-{tr_sub.settings_revision_date}"):
                tr_sub.send_modmail(subject = "[Notification] wiki settings loading error"
                             f"There was an error loading your {BOT_NAME} configuration: {{status}} "
                             f"\n\n https://www.reddit.com/r/{{subreddit_name}}"
                             f"/wiki/edit/{BOT_NAME}. \n\n"
                             f"Please see https://www.reddit.com/r/ModeratelyHelpfulBot/wiki/index for examples")
                record_actioned(f"wu-{subreddit_name}-{tr_sub.settings_revision_date}")
        else:
            to_update_db = True
    if to_update_db:
        s.add(tr_sub)
        s.commit()
    return tr_sub


def purge_old_records():
    purge_statement = "delete t  from RedditPost t inner join TrackedSubs s on t.subreddit_name = s.subreddit_name where  t.time_utc  < utc_timestamp() - INTERVAL greatest(s.min_post_interval_mins, 60*24*14) MINUTE  and t.flagged_duplicate=0 and t.pre_duplicate=0"
    _ = s.execute(purge_statement)


def purge_old_records_by_subreddit(tr_sub: TrackedSubreddit):
    print("looking for old records to purge from ", tr_sub.subreddit_name, tr_sub.min_post_interval)
    _ = s.query(SubmittedPost).filter(
        SubmittedPost.time_utc < datetime.now(pytz.utc).replace(tzinfo=None) - tr_sub.min_post_interval,
        SubmittedPost.counted_status != CountedStatus.FLAGGED.value, SubmittedPost.pre_duplicate.is_(False),
        SubmittedPost.subreddit_name == tr_sub.subreddit_name).delete()

    # SubmittedPost.flagged_duplicate.is_(False),
    # print("purging {} old records from {}", len(to_delete), tr_sub.subreddit_name)
    # to_delete.delete()
    s.commit()


def check_new_submissions(query_limit=800):
    global REDDIT_CLIENT
    subreddit_names = []
    subreddit_names_complete = []
    logger.info("pulling new posts!")
    possible_new_posts = [a for a in REDDIT_CLIENT.subreddit('mod').new(limit=query_limit)]

    count = 0
    for post_to_review in possible_new_posts:

        subreddit_name = str(post_to_review.subreddit).lower()
        if subreddit_name in subreddit_names_complete:
            # print(f'done w/ {subreddit_name}')
            continue
        previous_post: SubmittedPost = s.query(SubmittedPost).get(post_to_review.id)
        if previous_post:
            subreddit_names_complete.append(subreddit_name)
            continue
        if not previous_post:
            post = SubmittedPost(post_to_review)
            if subreddit_name not in subreddit_names:
                subreddit_names.append(subreddit_name)
            s.add(post)
            count += 1
    logger.info(f'found {count} posts')
    logger.debug("updating database...")
    s.commit()
    return subreddit_names


def check_spam_submissions(subreddit_name='mod'):
    global REDDIT_CLIENT
    possible_spam_posts = []
    try:
        possible_spam_posts = [a for a in REDDIT_CLIENT.subreddit(subreddit_name).mod.spam(only='submissions')]
    except prawcore.exceptions.Forbidden:
        pass
    for post_to_review in possible_spam_posts:
        previous_post: SubmittedPost = s.query(SubmittedPost).get(post_to_review.id)
        if previous_post:
            break
        if not previous_post:
            post = SubmittedPost(post_to_review)
            subreddit_name = post.subreddit_name.lower()
            # logger.info("found spam post: '{0}...' http://redd.it/{1} ({2})".format(post.title[0:20], post.id,
            #                                                                         subreddit_name))

            # post.reviewed = True
            s.add(post)
            subreddit_author: SubAuthor = s.query(SubAuthor).get((subreddit_name, post.author))
            if subreddit_author and subreddit_author.hall_pass >= 1:
                subreddit_author.hall_pass -= 1
                post.api_handle.mod.approve()
                s.add(subreddit_author)
    s.commit()


def main_loop():
    load_settings()

    users_to_check = []
    for u in users_to_check:
        u1 = TrackedAuthor(u)
        print(u1.calculate_nsfw())
    i = 0

    """
    posts_to_check = s.query(SubmittedPost).filter(
        SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(hours=20),
        SubmittedPost.subreddit_name == "needafriend",
        SubmittedPost.counted_status < 3) \
        .order_by(desc(SubmittedPost.time_utc)) \
        .all()
    for post in posts_to_check:
        age = get_age(post.title)
        if 25 > age > 13:
            post.post_flair = "strict sfw"
    """

    # .filter(or_(SubmittedPost.nsfw_last_checked < datetime.now(pytz.utc) - timedelta(hours=5),
    #    SubmittedPost.nsfw_last_checked == False)) \
    while True:
        print('start_loop')
        try:
            i += 1
            check_new_submissions()
            check_spam_submissions()

            start = datetime.now(pytz.utc)

            # only do this if not too busy
            # if last_index < 30:

            look_for_rule_violations2(do_cleanup=(i % 15 == 0))  # uses a lot of resources

            if i % 75 == 0:
                purge_old_records()

            print("$$$checking rule violations took this long", datetime.now(pytz.utc) - start)

            # update_TMBR_submissions(look_back=timedelta(days=7))
            send_broadcast_messages()
            #  do_automated_replies()  This is currently disabled!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            handle_direct_messages()
            handle_modmail_messages()

            nsfw_checking()
        except prawcore.exceptions.ServerError:
            import time
            time.sleep(60*5) # sleep for a bit server errors
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            print(trace)
            TrackedSubreddit.get_subreddit_by_name(BOT_NAME).send_modmail(subject="[Notification] MHB Exception",
                             body=trace)



def nsfw_checking():  # Does not expand comments

    posts_to_check = s.query(SubmittedPost).filter(
        SubmittedPost.post_flair.ilike("%strict sfw%"),
        SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(hours=36),
        SubmittedPost.counted_status < 3) \
        .order_by(desc(SubmittedPost.time_utc)) \
        .all()

    # .filter(or_(SubmittedPost.nsfw_last_checked < datetime.now(pytz.utc) - timedelta(hours=5),
    #    SubmittedPost.nsfw_last_checked == False)) \
    # send_modmail(tr_sub, "\n\n".join(response_lines), recent_post=recent_post, prev_post=possible_repost)
    #or_(SubmittedPost.nsfw_last_checked < datetime.now(pytz.utc) - timedelta(hours=1),
    #    SubmittedPost.nsfw_last_checked == False),

    author_list = dict()
    tick = datetime.now()

    for post in posts_to_check:

        op_age = get_age(post.title)

        tock = datetime.now()
        if tock-tick > timedelta(minutes=3):
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
                          "Plwase see https://www.reddit.com/r/NewToReddit/wiki/" \
                          "ntr-guidetoreddit#wiki_part_7.3A__safety_on_reddit for more safety tips."
        sticky_post = "This post has been flaired 'strict sfw'. This means that any user who contacts the poster " \
                      "may be permanently banned from the subreddit if they have a heavily NSFW history (>20%) " \
                      "or are using new throwaway accounts. This subreddit is strictly for platonic friendships, and " \
                      "the mods will not tolerate the solicitation of minors or otherwise unwanted harassment. " \
                      "The moderators will still evaluate all bans on a case by case basis prior to action taken."
        if post.time_utc.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc)-timedelta(hours=3) \
                and op_age < 18 and post.nsfw_repliers_checked == False:
            try:
                comment_reply = post.get_api_handle().reply(sticky_post)
                comment_reply.mod.distinguish()
                comment_reply.mod.approve()
            except (praw.exceptions.APIException, prawcore.exceptions.Forbidden, ):
                pass
            try:
                REDDIT_CLIENT.redditor(post.author).message("Strict SFW Mode", warning_message)
                dms_disabled = "NOT DISABLED"
            except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                dms_disabled = "disabled"

            post.nsfw_repliers_checked = True
            s.add(post)
        tr_sub = TrackedSubreddit.get_subreddit_by_name(post.subreddit_name)
        time_since: timedelta = datetime.now(pytz.utc)-post.nsfw_last_checked.replace(tzinfo=timezone.utc)
        time_since_hrs = int(time_since.total_seconds()/3600)
        # longer and longer between checks.

        if post.nsfw_last_checked.replace(tzinfo=timezone.utc) < datetime.now(pytz.utc)-timedelta(hours=int(time_since_hrs*0.5*time_since_hrs)):
            # print("checked recently...")
            continue
        print(
            f"checking post: {post.subreddit_name} {post.title} {post.time_utc} {post.get_comments_url()} {post.post_flair}")

        #print(f"post: {post.subreddit_name} {post.title} {post.time_utc} {post.get_comments_url()} {post.post_flair} dm disabled? {dms_disabled}")

        top_level_comments: List[praw.models.Comment ] = list(post.get_api_handle().comments)
        for c in top_level_comments:
            author = None
            author_name = None

            if hasattr(c, 'author') and c.author and hasattr(c.author, 'name'):
                author_name = c.author.name
                if author_name in tr_sub.get_mods_list():
                    continue
                if author_name in author_list:
                    continue

                author: TrackedAuthor = s.query(TrackedAuthor).get(c.author.name)
                if not author:
                    author = TrackedAuthor(c.author.name)

            if author:
                author_list[author_name] = author

                if author.nsfw_pct == -1 or not author.last_calculated\
                        or author.last_calculated.replace(tzinfo=timezone.utc) < (datetime.now(pytz.utc) - timedelta(days=7)):
                    nsfw_pct = author.calculate_nsfw()
                    s.add(author)
                    try:
                        tr_sub.get_api_handle().flair.set(author_name, text=f"{int(nsfw_pct)}% NSFW")
                    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                        pass

                #if author.has_nsfw_post:
                #    try:
                #        tr_sub.banned.add(author.author_name, ban_note=f"Has NSFW post history {author.has_nsfw_post}",
                #                          ban_message="NO HISTORY OF NSFW POSTS ALLOWED IN /r/needafriend")
                #    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                #        pass

                if not check_actioned(f"comment-{c.id}") and (
                        (author.nsfw_pct > 80 or (op_age < 18 and author.age and author.age > 18)
                         or (op_age < 18 and author.nsfw_pct > 10) or author.has_nsfw_post)):
                    comment_url = f"https://www.reddit.com/r/{post.subreddit_name}/comments/{post.id}/-/{c.id}"
                    response = f"Author very nsfw: http://www.reddit.com/u/{author_name} . " \
                               f"Commented on: {post.get_comments_url()} \n\n. " \
                               f"Link to comment: {comment_url} \n\n. " \
                               f"Poster's age {op_age}. Commenter's age {author.age} \n\n" \
                               f"Has nsfw post? {author.has_nsfw_post}"
                    subject = f"[Notification] Found this potential predator {author_name} score={int(author.nsfw_pct)}"
                    print(response)
                    try:
                        c.mod.remove()
                    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                        pass
                    tr_sub.send_modmail(subject=subject, body=response)

                    REDDIT_CLIENT.redditor(BOT_OWNER).message("pervy perv", response)
                    record_actioned(f"comment-{c.id}")
        post.nsfw_repliers_checked = True
        post.nsfw_last_checked = datetime.now(pytz.utc)
        s.add(post)
        s.commit()



def get_naughty_list():
    authors_tuple = s.query(SubmittedPost.author, SubmittedPost.subreddit_name,
                            func.count(SubmittedPost.author).label('qty')) \
        .filter(
        SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(days=30)) \
        .group_by(SubmittedPost.author, SubmittedPost.subreddit_name).order_by(desc('qty')).limit(80)

    for x, y, z in authors_tuple:
        print(f"{x}\t{y}\t{z}")
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
    # formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
BOT_SUB = TrackedSubreddit.get_subreddit_by_name(BOT_NAME)

main_loop()
