#!/usr/bin/env python3.6
import logging
from datetime import datetime, timedelta, timezone
from typing import List

import humanize
import iso8601
import praw
import prawcore
import pytz
import yaml
from praw import exceptions
from praw.models import Submission
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from settings import BOT_NAME, BOT_PW, CLIENT_ID, CLIENT_SECRET, BOT_OWNER, DB_ENGINE

"""
To do list:
asyncio 
incorporate toolbox? https://www.reddit.com/r/nostalgia/wiki/edit/toolbox check usernotes?
upgrade python and praw version
clean actioned comments after 3 months
fix logging
"""

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


class SubmittedPost(Base):
    __tablename__ = 'RedditPost'
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    author = Column(String(21), nullable=True)
    submission_text = Column(String(191), nullable=True)
    time_utc = Column(DateTime, nullable=False)
    subreddit_name = Column(String(21), nullable=True)
    banned_by = Column(String(21), nullable=True)
    flagged_duplicate = Column(Boolean, nullable=True)
    pre_duplicate = Column(Boolean, nullable=True)
    self_deleted = Column(Boolean, nullable=True)
    reviewed = Column(Boolean, nullable=True)
    last_checked = Column(DateTime, nullable=False)
    bot_comment_id = Column(String(10), nullable=True)  # don't need this?
    is_self = Column(Boolean, nullable=True)
    removed_status = Column(String(21), nullable=True)
    post_flair = Column(String(21), nullable=True)
    author_flair = Column(String(42), nullable=True)
    counted_status = Column(SmallInteger, nullable=True, default=-1)  # not-checked=-1,  does_not_count=0, does count=1
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
        self.counted_status = -1
        self.post_flair = submission.link_flair_text
        self.author_flair = submission.author_flair_text

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

    def mod_remove(self) -> bool:
        try:
            self.get_api_handle().mod.remove()
            return True
        except praw.exceptions.APIException:
            logger.warning(f'something went wrong removing post: http://redd.it/{self.id}')
            return False
        except prawcore.exceptions.Forbidden:
            logger.warning(f'I was not allowed to remove the post: http://redd.it/{self.id}')
            return False

    def reply(self, response, distinguish=True, approve=False, lock_thread=True):
        try:
            comment = self.get_api_handle().reply(response)
            if lock_thread:
                self.get_api_handle().mod.lock()
            if distinguish:
                comment.mod.distinguish()
            if approve:
                comment.mod.approve()
            return comment
        except praw.exceptions.APIException:
            logger.warning(f'Something with replying to this post: http://redd.it/{self.id}')
            return False
        except prawcore.exceptions.Forbidden:
            logger.warning(f'Something with replying to this post:: http://redd.it/{self.id}')
            return False

    def get_status(self) -> str:
        _ = self.get_api_handle()
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

    def update(self, reviewed=None, flagged_duplicate=None, counted_status=None):
        if reviewed is not None:
            self.reviewed = reviewed
        if flagged_duplicate is not None:
            self.flagged_duplicate = flagged_duplicate
        if counted_status is not None:
            self.counted_status = counted_status
        self.last_checked = datetime.now(pytz.utc)


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
    last_updated = Column(DateTime, nullable=True, default=datetime.now())
    next_eligible = Column(DateTime, nullable=True, default=datetime(2019, 1, 1, 0, 0))
    ban_last_failed = Column(DateTime, nullable=True)
    hall_pass = Column(Integer, default=0)
    last_valid_post = Column(String(10))

    def __init__(self, subreddit_name: str, author_name: str):
        self.subreddit_name = subreddit_name
        self.author_name = author_name


class TrackedSubreddit(Base):
    __tablename__ = 'TrackedSubs7'
    subreddit_name = Column(String(21), nullable=False, primary_key=True)
    checking_mail_enabled = Column(Boolean, nullable=True)
    settings_yaml_txt = Column(UnicodeText, nullable=True)
    settings_yaml = None
    last_updated = Column(DateTime, nullable=True)
    last_error_msg = Column(DateTime, nullable=True)
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
    is_nsfw = Column(Boolean, nullable=False, default=0)

    subreddit_mods = []
    rate_limiting_enabled = False
    min_post_interval_hrs = 72
    min_post_interval = timedelta(hours=72)
    grace_period_mins = timedelta(minutes=30)
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
    approve = False
    blacklist_enabled = True
    lock_thread = True
    comment_stickied = False
    title_not_exempt_keyword = None
    canned_responses = {}

    def __init__(self, subreddit_name: str):
        self.subreddit_name = subreddit_name.lower()
        self.save_text = False
        self.last_updated = datetime(2019, 1, 1, 0, 0)
        self.error_message = datetime(2019, 1, 1, 0, 0)
        self.update_from_yaml(force_update=True)
        self.settings_revision_date = None

    def get_mods_list(self, subreddit_handle=None) -> List[str]:
        subreddit_handle = REDDIT_CLIENT.subreddit(self.subreddit_name) if not subreddit_handle else subreddit_handle
        try:
            return list(moderator.name for moderator in subreddit_handle.moderator())
        except prawcore.exceptions.NotFound:
            return []

    def update_from_yaml(self, force_update: bool = False) -> (Boolean, String):
        return_text = "Updated Successfully!"
        subreddit_handle = REDDIT_CLIENT.subreddit(self.subreddit_name)
        self.is_nsfw = subreddit_handle.over18
        self.ban_ability = -1
        self.subreddit_mods = self.get_mods_list(subreddit_handle=subreddit_handle)

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
                'message': str,
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
                    # if not isinstance(pr_settings[pr_setting], possible_settings[pr_setting]):
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
            self.min_post_interval_mins = self.min_post_interval.total_seconds() // 60
            if 'grace_period_mins' in pr_settings and pr_settings['grace_period_mins'] is not None:
                self.grace_period_mins = timedelta(minutes=pr_settings['grace_period_mins'])
            if not self.ban_threshold_count:
                self.ban_threshold_count = 5

        if 'modmail' in self.settings_yaml:
            m_settings = self.settings_yaml['modmail']
            possible_settings = ('modmail_no_posts_reply', 'modmail_no_posts_reply_internal', 'modmail_posts_reply',
                                 'modmail_auto_approve_messages_with_links', 'modmail_all_reply',
                                 'modmail_notify_replied_internal', 'modmail_no_link_reply', 'canned_responses',)
            if m_settings:
                for m_setting in m_settings:
                    if m_setting in possible_settings:
                        setattr(self, m_setting, m_settings[m_setting])
                    else:
                        return_text = "Did not understand variable '{}'".format(m_setting)
            if self.subreddit_name.lower() == "puppy101":
                self.modmail_notify_replied_internal = False
        if not self.min_post_interval:
            self.min_post_interval = timedelta(hours=72)
        if not self.grace_period_mins:
            self.grace_period_mins = timedelta(minutes=30)

        if not self.max_count_per_interval:
            self.max_count_per_interval = 1

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
            tr_sub.update_from_yaml(force_update=False)  # load variables from stored yaml
        return tr_sub

    def get_author_summary(self, author_name: str) -> str:
        if author_name.startswith('u/'):
            author_name = author_name.replace("u/", "")

        recent_posts = s.query(SubmittedPost).filter(
            SubmittedPost.subreddit_name.ilike(self.subreddit_name),
            SubmittedPost.author == author_name,
            SubmittedPost.time_utc > datetime.now() - timedelta(days=182)).all()
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
                                                    post.get_status()))
            diff = post.time_utc
        response_lines.append("Current settings: {} post(s) per {} hour(s)"
                              .format(self.max_count_per_interval, self.min_post_interval_hrs))
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

    def send_modmail(self, subject=f"Message from {BOT_NAME}", body="Something here"):
        try:
            REDDIT_CLIENT.subreddit(self.subreddit_name).message(subject, body)
        except (praw.exceptions.APIException, prawcore.exceptions.Forbidden, AttributeError):
            logger.warning('something went wrong in sending modmail')


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


def already_has_bot_comment(submission: praw.models.Submission):  # repurpose to find explanatory comment
    global REDDIT_CLIENT
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
                SubmittedPost.subreddit_name.ilike(tr_sub.subreddit_name),
                SubmittedPost.time_utc > recent_post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins,
                SubmittedPost.time_utc < recent_post.time_utc,  # posts not after post in question
                SubmittedPost.id != recent_post.id,  # not the same post id
                SubmittedPost.author == recent_post.author,  # same author
                SubmittedPost.counted_status != 0) \
        .order_by(SubmittedPost.time_utc) \
        .all()

    # Filter possible reposts (some maybe removed by automoderator or within grace period) - can't do in database
    most_recent_reposts = []
    i = 0
    for possible_repost in possible_reposts:
        i += 1
        if i > 20:
            break
        logger.info("possible repost of: {0}... http://redd.it/{1} {2} already counted? {3}".format(
            possible_repost.title[0:20],
            possible_repost.id,
            datetime.now().replace(tzinfo=timezone.utc) - possible_repost.time_utc.replace(tzinfo=timezone.utc),
            possible_repost.counted_status))

        # Need to check if it counts if not already checked:
        if possible_repost.counted_status == -1:  # so it was not previously checked
            possible_repost.counted_status, result = check_for_post_exemptions(tr_sub, possible_repost)  # check it

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
    logger.info(">>>total {} max {} query time: {}".format(
        len(most_recent_reposts),
        tr_sub.max_count_per_interval,
        datetime.now() - tick
    ))
    return most_recent_reposts


def check_for_post_exemptions(tr_sub: TrackedSubreddit, recent_post: SubmittedPost):
    # check if removed
    banned_by = recent_post.get_api_handle().banned_by
    # logger.debug(">>>>exemption status: {}".format(banned_by))
    if banned_by is True or ((tr_sub.ignore_AutoModerator_removed and banned_by == "AutoModerator")
                             or (tr_sub.ignore_moderator_removed and banned_by in tr_sub.subreddit_mods)):
        # logger.debug("...already removed")
        return 0, "post is removed by a mod"

    # check if oc exempt:
    if tr_sub.exempt_oc and recent_post.get_api_handle().is_original_content:
        logger.debug("...oc exempt")
        return 0, "oc exempt"

    # Check if any post type restrictions
    is_self = recent_post.get_api_handle().is_self
    if is_self is True and tr_sub.exempt_self_posts is True:
        logger.debug("...self_post exempt")
        return 0, "self_post_exempt"
    if is_self is not True and tr_sub.exempt_link_posts is True:
        logger.debug("...link_post exempt")
        return 0, "link_post_exempt"

    # check if flair-exempt
    author_flair = recent_post.get_api_handle().author_flair_text
    # add CSS class to author_flair
    if author_flair and recent_post.get_api_handle().author_flair_css_class:
        author_flair = author_flair + recent_post.get_api_handle().author_flair_css_class

    # Flair keyword exempt
    if tr_sub.author_exempt_flair_keyword and isinstance(tr_sub.author_exempt_flair_keyword, str) \
            and author_flair and tr_sub.author_exempt_flair_keyword in author_flair:
        logger.debug(">>>flair exempt")
        return 0, "flair exempt {}".format(author_flair)

    # Not-flair-exempt keyword (Only restrict certain flairs)
    if tr_sub.author_not_exempt_flair_keyword \
            and ((author_flair and tr_sub.author_not_exempt_flair_keyword not in author_flair) or not author_flair):
        return 0, "flair not exempt {}".format(author_flair)

    # check if title keyword exempt:
    if tr_sub.title_exempt_keyword:
        flex_title = recent_post.title.lower()
        if (isinstance(tr_sub.title_exempt_keyword, str)
            and tr_sub.title_exempt_keyword.lower() in flex_title) or \
                (isinstance(tr_sub.title_exempt_keyword, list)
                 and any(x in flex_title for x in [y.lower() for y in tr_sub.title_exempt_keyword])):
            logger.debug(">>>title keyword exempted")
            return 0, "title keyword  exempt {}".format(tr_sub.title_exempt_keyword)

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
            return 0, "title keyword not exempt {}".format(tr_sub.title_exempt_keyword)

    # Ignore posts by mods
    if tr_sub.exempt_moderator_posts is True and recent_post.author in tr_sub.subreddit_mods:
        logger.debug(">>>mod exempt")
        return 0, "moderator exempt"

    return 1, "no exemptions"


def look_for_rule_violations(do_cleanup: bool = False):
    global REDDIT_CLIENT
    global WATCHED_SUBS
    logger.debug("querying recent post(s)")

    faster_statement = "select max(t.id), group_concat(t.id order by t.id), group_concat(t.reviewed order by t.id), t.author, t.subreddit_name, count(t.author), max( t.time_utc), t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60 from RedditPost t inner join TrackedSubs7 s on t.subreddit_name = s.subreddit_name where counted_status !=0 and t.time_utc> utc_timestamp() - Interval s.min_post_interval_mins  minute and t.time_utc > utc_timestamp() - Interval 72 hour group by t.author, t.subreddit_name having count(t.author) > s.max_count_per_interval and (max(t.time_utc)> max(t.last_checked) or max(t.last_checked) is NULL) order by max(t.time_utc) desc ;"

    more_accurate_statement = "SELECT MAX(t.id), GROUP_CONCAT(t.id ORDER BY t.id), GROUP_CONCAT(t.reviewed ORDER BY t.id), t.author, t.subreddit_name, COUNT(t.author), MAX(t.time_utc) as most_recent, t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60 FROM RedditPost t INNER JOIN TrackedSubs7 s ON t.subreddit_name = s.subreddit_name WHERE counted_status !=0 AND t.time_utc > utc_timestamp() - INTERVAL s.min_post_interval_mins MINUTE  GROUP BY t.author, t.subreddit_name HAVING COUNT(t.author) > s.max_count_per_interval AND most_recent > utc_timestamp() - INTERVAL 72 HOUR AND (most_recent > MAX(t.last_checked) or max(t.last_checked) is NULL) ORDER BY most_recent desc ;"
    recent_posts = list()

    tick = datetime.now()
    if do_cleanup:
        print("doing more accurate")
        rs = s.execute(more_accurate_statement)
    else:
        print("doing usual")
        rs = s.execute(faster_statement)
    print(f"query took this long {datetime.now() - tick}")

    tick = datetime.now(pytz.utc)

    max_index = 0
    for row in rs:
        print(row[0], row[1], row[2], row[3], row[4])
        # post = s.query(SubmittedPost).get(row[0])
        predecessors = row[1].split(',')
        predecessors_times = row[2].split(',')

        for p, t in zip(predecessors, predecessors_times):
            # print(p,t)
            if t == "0":  # dd if not yet reviewed
                post = s.query(SubmittedPost).get(p)
                if post:
                    recent_posts.append(post)
                else:
                    print("could not find", p)
        max_index += 1

    logger.info(f"found {max_index} to review")

    last_author = "blah"
    for index, recent_post in enumerate(recent_posts):
        tock = datetime.now(pytz.utc) - tick

        # Cut short if taking too long, but not if in the middle of the same author
        if tock > timedelta(minutes=3) and last_author != recent_post.author and do_cleanup is False:
            logger.debug("Aborting, taking more than 3 min")
            s.commit()
            break

        last_author = recent_post.author

        # Periodically save work to db
        if (index + 1) % 40 == 0:
            s.commit()
            logger.info("$$$ -{0} current time".format(
                datetime.now(pytz.utc).replace(tzinfo=timezone.utc) - recent_post.time_utc.replace(
                    tzinfo=timezone.utc)))
            logger.debug("           %d of %d" % (index, max_index))

        # Load subreddit settings
        subreddit_name = recent_post.subreddit_name.lower()
        tr_sub = update_list_with_subreddit(subreddit_name, request_update_if_needed=True)

        # Check if they're on the watchlist
        # recent_post.author = recent_post.author.lower()
        subreddit_author: SubAuthor = s.query(SubAuthor).get((subreddit_name, recent_post.author))

        # check for hall pass  - should be done when querying posts instead?
        if subreddit_author and subreddit_author.hall_pass > 0:
            subreddit_author.hall_pass -= 1
            s.add(subreddit_author)
            recent_post.reviewed = True
            recent_post.last_checked = datetime.now(pytz.utc)
            recent_post.counted_status = 1
            s.add(recent_post)
            notification_text = f"Hall pass was used by {subreddit_author.author_name}: http://redd.it/{recent_post.id}"
            REDDIT_CLIENT.redditor(BOT_OWNER).message(subreddit_name, notification_text)
            tr_sub.send_modmail(body= notification_text)
            logger.debug(">>>hallpassed")
            continue

        if subreddit_author and subreddit_author.next_eligible:
            print(f"next eligible {subreddit_author.next_eligible}")
        if subreddit_author and subreddit_author.next_eligible and subreddit_author.next_eligible.replace(
                tzinfo=timezone.utc) > datetime.now(pytz.utc):
            was_successful = recent_post.mod_remove()
            if was_successful:
                try:
                    if tr_sub.comment:
                        print('last_valid_post', subreddit_author.last_valid_post)
                        last_valid_post: SubmittedPost = s.query(SubmittedPost).get(
                            subreddit_author.last_valid_post) if subreddit_author.last_valid_post is not None else None
                        make_comment(tr_sub, recent_post, [last_valid_post, ],
                                     tr_sub.comment, distinguish=tr_sub.distinguish, approve=tr_sub.approve,
                                     lock_thread=tr_sub.lock_thread, stickied=tr_sub.comment_stickied,
                                     next_eligibility=subreddit_author.next_eligible, blacklist=True)
                except (praw.exceptions.APIException, prawcore.exceptions.Forbidden) as e:
                    logger.warning('something went wrong in creating comment %s', str(e))

                recent_post.reviewed = True
                recent_post.flagged_duplicate = True
                recent_post.last_checked = datetime.now(pytz.utc)
                recent_post.counted_status = 3

                logger.info(f"post removed - prior to eligibility for user {recent_post.author} {recent_post.get_url()}"
                            f" {recent_post.subreddit_name} {recent_post.title}")
                s.add(subreddit_author)
                s.add(recent_post)
                continue
            else:
                # Maybe recheck permissions if not allowed to remove posts
                tr_sub.update_from_yaml(force_update=True)

            if subreddit_name not in WATCHED_SUBS:
                print(f"CANNOT FIND SUBREDDIT!!! {recent_post.subreddit_name}")
                continue

        # check for post exemptions
        counted_status, result = check_for_post_exemptions(tr_sub, recent_post)
        logger.info("does this {} count? {}".format(recent_post.get_url(), result))
        since_post = datetime.now(pytz.utc) - recent_post.time_utc.replace(tzinfo=timezone.utc)
        logger.info(
            f"=================================================\n"
            f"{index}-Checking '{recent_post.title[0:20]}...' by '{recent_post.author}' http://redd.it/{recent_post.id}"
            f" subreddit:({recent_post.subreddit_name}): >-{since_post}<")
        if counted_status == 0:
            recent_post.counted_status = 0
            recent_post.reviewed = True
            recent_post.last_checked = datetime.now(pytz.utc)
            s.add(recent_post)
            continue
        associated_reposts = find_previous_posts(tr_sub, recent_post)
        verified_reposts_count = len(associated_reposts)

        # Now check if actually went over threshold
        if verified_reposts_count >= tr_sub.max_count_per_interval:
            logger.info("----------------post time {0} | interval {1}  after {2} sub:{3}".format(
                datetime.now(pytz.utc) - recent_post.time_utc.replace(tzinfo=timezone.utc),
                tr_sub.min_post_interval,
                recent_post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins,
                recent_post.subreddit_name, recent_post.time_utc))

            do_requested_action_for_valid_reposts(tr_sub, recent_post, associated_reposts)
            recent_post.flagged_duplicate = True
            # Keep preduplicate posts to keep track of later
            for post in associated_reposts:
                post.pre_duplicate = True
                s.add(post)
            check_for_actionable_violations(tr_sub, recent_post, associated_reposts)
        recent_post.reviewed = True
        recent_post.last_checked = datetime.now(pytz.utc)
        s.add(recent_post)
    s.commit()
    return


def look_for_rule_violations2(do_cleanup: bool = False):
    global REDDIT_CLIENT
    global WATCHED_SUBS
    logger.debug("querying recent post(s)")

    faster_statement = "select max(t.id), group_concat(t.id order by t.id), group_concat(t.reviewed order by t.id), t.author, t.subreddit_name, count(t.author), max( t.time_utc), t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60 from RedditPost t inner join TrackedSubs7 s on t.subreddit_name = s.subreddit_name where counted_status !=0 and t.time_utc> utc_timestamp() - Interval s.min_post_interval_mins  minute and t.time_utc > utc_timestamp() - Interval 72 hour group by t.author, t.subreddit_name having count(t.author) > s.max_count_per_interval and (max(t.time_utc)> max(t.last_checked) or max(t.last_checked) is NULL) order by max(t.time_utc) desc ;"

    more_accurate_statement = "SELECT MAX(t.id), GROUP_CONCAT(t.id ORDER BY t.id), GROUP_CONCAT(t.reviewed ORDER BY t.id), t.author, t.subreddit_name, COUNT(t.author), MAX(t.time_utc) as most_recent, t.reviewed, t.flagged_duplicate, s.is_nsfw, s.max_count_per_interval, s.min_post_interval_mins/60 FROM RedditPost t INNER JOIN TrackedSubs7 s ON t.subreddit_name = s.subreddit_name WHERE counted_status !=0 AND t.time_utc > utc_timestamp() - INTERVAL s.min_post_interval_mins MINUTE  GROUP BY t.author, t.subreddit_name HAVING COUNT(t.author) > s.max_count_per_interval AND most_recent > utc_timestamp() - INTERVAL 72 HOUR AND (most_recent > MAX(t.last_checked) or max(t.last_checked) is NULL) ORDER BY most_recent desc ;"

    tick = datetime.now()
    if do_cleanup:
        print("doing more accurate")
        rs = s.execute(more_accurate_statement)
    else:
        print("doing usual")
        rs = s.execute(faster_statement)
    print(f"query took this long {datetime.now() - tick}")

    tick = datetime.now(pytz.utc)

    for i, row in enumerate(rs):
        print(f"========================{i}=================================")
        print(row[0], row[1], row[2], row[3], row[4])
        post_ids = row[1].split(',')
        posts = []
        for post_id in post_ids:
            posts.append(s.query(SubmittedPost).get(post_id))
        author_name: str = row[3]
        subreddit_name: str = row[4].lower()

        # Break if taking too long
        tock = datetime.now(pytz.utc) - tick
        if tock > timedelta(minutes=3) and do_cleanup is False:
            logger.debug("Aborting, taking more than 3 min")
            s.commit()
            break

        # Load subreddit settings
        tr_sub = update_list_with_subreddit(subreddit_name, request_update_if_needed=True)
        max_count = tr_sub.max_count_per_interval

        # Check if they're on the soft blacklist
        subreddit_author: SubAuthor = s.query(SubAuthor).get((subreddit_name, author_name))

        # Remove any posts that are prior to eligibility
        left_over_posts = []
        print(f"---max_count: {max_count}, interval:{tr_sub.min_post_interval_hrs} "
              f"grace_period:{tr_sub.grace_period_mins}")
        for j, post in enumerate(posts):

            logger.info(f"{i}-{j}Checking: r/{subreddit_name}  {author_name}  {post.time_utc}  "
                        f"url:{post.get_url()}  title:{post.title[0:30]}")
            # Check for soft blacklist

            if subreddit_author and post.time_utc < subreddit_author.next_eligible:
                post.update(reviewed=True, flagged_duplicate=True, counted_status=3)
                s.add(post)
                logger.info(
                    f"{i}-{j}\t\tpost removed - prior to eligibility")
                try:
                    post.mod_remove()
                    if tr_sub.comment:
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
                logger.info(f"\t\tpost status: {result}")
                if counted_status == 0:
                    post.update(counted_status=0)
                    s.add(post)
                else:
                    left_over_posts.append(post)
            else:
                logger.info(f"{i}-{j}\t\tpost status: already reviewed")

        """
        # Skip if we don't need to go through each post
        if len(left_over_posts) < max_count:
            logger.info("Did not collect enough counted posts")
            s.commit()
            continue
        """



        # Collect all relevant posts
        back_posts = s.query(SubmittedPost) \
            .filter(SubmittedPost.flagged_duplicate.is_(False),
                    SubmittedPost.subreddit_name.ilike(tr_sub.subreddit_name),
                    SubmittedPost.time_utc > posts[0].time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins,
                    SubmittedPost.time_utc < posts[-1].time_utc,  # posts not after last post in question
                    SubmittedPost.author == author_name,
                    SubmittedPost.counted_status != 0) \
            .order_by(SubmittedPost.time_utc) \
            .all()

        possible_pre_posts = []
        logger.info(f"Found {len(back_posts)} backposts")
        if len(back_posts) == 0:
            logger.info("Nothing to do, moving on.")
            break
        # Look for exempted posts
        for j, post in enumerate(back_posts):
            logger.info(f"{i}-{j} Backpost: r/{subreddit_name}  {author_name}  {post.time_utc}  "
                        f"url:{post.get_url()}  title:{post.title[0:30]}")
            if post.counted_status == 1:
                logger.info(f"\tpost_counted_status counted status: {post.counted_status} Including")
                possible_pre_posts.append(post)
            elif post.counted_status == -1:
                counted_status, result = check_for_post_exemptions(tr_sub, post)
                if counted_status == 0:
                    logger.info(f"\tpost exempt: {counted_status} {result}")
                    post.update(counted_status=0)
                    s.add(post)
                elif counted_status == 1:
                    logger.info(f"\tpost not exempt:{counted_status}  {result}")
                    post.update(counted_status=1)
                    possible_pre_posts.append(post)
                    s.add(post)
                else:
                    logger.info(f"\tcounted status:{counted_status}  {result}. Not including")
                    post.update(counted_status=counted_status)
                    s.add(post)
            else:
                logger.info(f"\tpost_counted_status counted status: {post.counted_status}. Not including")


        for j, post in enumerate(left_over_posts):
            logger.info(f"{i}-{j} Reviewing: r/{subreddit_name}  {author_name}  {post.time_utc}  "
                        f"url:{post.get_url()}  title:{post.title[0:30]}")
            if post.reviewed:
                print("\tAlready reviewed - skipping")
                continue

            # filter those that are relevant to this particular post (within range but not in grace period)
            """
            associated_reposts = [x for x in relevant_posts if x.time_utc < post.time_utc and
                                  (x.time_utc > post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins)
                                  and not (x.get_status() == "self-deleted"  # grace period thing
                                           and post.time_utc - x.time_utc < tr_sub.grace_period_mins)]
            """
            associated_reposts = []
            for x in possible_pre_posts:
                print(f"\tpost time:{post.time_utc} prev:{x.time_utc} furthestback: {post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins}")
                if x.id == post.id:
                    print("\t\t Same post")
                    continue
                if x.time_utc < post.time_utc - tr_sub.min_post_interval + tr_sub.grace_period_mins or x.time_utc > post.time_utc:
                    print("\t\t Not in correct time period")
                    continue
                status = x.get_status()
                print(f"\t\tpost status: {status} grace_period:{tr_sub.grace_period_mins} diff: {post.time_utc - x.time_utc}")
                if status == "self-deleted" and post.time_utc - x.time_utc < tr_sub.grace_period_mins:
                    print("\t\t Grace period exempt")
                associated_reposts.append(x)



            if len(associated_reposts) < tr_sub.max_count_per_interval:
                logger.info(f"\tNot enough previous posts: {len(associated_reposts)}/{max_count}: "
                            f"{','.join([x.id for x in associated_reposts])}")
                post.update(reviewed=True, counted_status=0)
                s.add(post)
                s.commit()
                continue

            # check for hall pass  - should be done when querying posts instead?
            if subreddit_author and subreddit_author.hall_pass > 0:
                subreddit_author.hall_pass -= 1
                notification_text = f"Hall pass was used by {subreddit_author.author_name}: http://redd.it/{post.id}"
                REDDIT_CLIENT.redditor(BOT_OWNER).message(subreddit_name, notification_text)
                tr_sub.send_modmail(subject="Hall pass was used", body=notification_text)
                logger.debug(">>>hallpassed")
                post.update(reviewed=True, counted_status=1, flagged_duplicate=False)
                s.add(subreddit_author)
                s.add(post)
            else:
                do_requested_action_for_valid_reposts(tr_sub, post, associated_reposts)
                post.update(reviewed=True, counted_status=1, flagged_duplicate=True)
                s.add(post)
                # Keep preduplicate posts to keep track of later
                for post in associated_reposts:
                    post.pre_duplicate = True
                    s.add(post)

                check_for_actionable_violations(tr_sub, post, associated_reposts)
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
        send_modmail(tr_sub, message, recent_post=recent_post, prev_post=possible_repost, )
    if tr_sub.action == "remove":
        post_status = recent_post.get_status()
        if post_status == "up":
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
            rp_reason = populate_tags(tr_sub.report_reason, recent_post, tr_sub=tr_sub,
                                      prev_post=possible_repost)
            recent_post.get_api_handle().report(("ModeratelyHelpfulBot:" + rp_reason)[0:99])
        else:
            recent_post.get_api_handle().report("ModeratelyHelpfulBot: repeatedly exceeding posting threshold")
    if tr_sub.message and recent_post.author:
        recent_post.get_api_handle().author.message("Regarding your post",
                                                    populate_tags(tr_sub.message, recent_post, tr_sub=tr_sub,
                                                                  prev_posts=most_recent_reposts))


def check_for_actionable_violations(tr_sub: TrackedSubreddit, recent_post: SubmittedPost,
                                    most_recent_reposts: List[SubmittedPost]):
    possible_repost = most_recent_reposts[-1]
    tick = datetime.now()
    other_spam_by_author = s.query(SubmittedPost).filter(
        SubmittedPost.flagged_duplicate.is_(True),
        SubmittedPost.author == recent_post.author,
        SubmittedPost.subreddit_name.ilike(tr_sub.subreddit_name),
        SubmittedPost.time_utc < recent_post.time_utc) \
        .all()

    logger.info("Author {0} had {1} rule violations. Banning if at least {2} - query time took: {3}"
                .format(recent_post.author, len(other_spam_by_author), tr_sub.ban_threshold_count,
                        datetime.now() - tick))

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
                f"per {tr_sub.min_post_interval_hrs} hour(s). "
                f"This {'does NOT' if tr_sub.ignore_moderator_removed else 'DOES'} include mod-removed posts. "
                f"While this post was within the rule, please do not make any new posts before "
                f"{most_recent_reposts[0].time_utc + tr_sub.min_post_interval} UTC, as it "
                f"may result in a ban. If you made a title mistake you have "
                f"STRICTLY {humanize.precisedelta(tr_sub.grace_period_mins)} to delete it and repost it. "
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
                      f"post(s) per {tr_sub.min_post_interval_hrs} hour(s), and it only allows for " \
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
                ban_message += f"\n\nYour ban will last {num_days} days from this message. " \
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

                send_modmail(tr_sub, "\n\n".join(response_lines), recent_post=recent_post, prev_post=possible_repost)
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


def populate_tags(input_text, recent_post, tr_sub=None, prev_post=None, prev_posts=None):
    if not isinstance(input_text, str):
        print("error: {0} is not a string".format(input_text))
        return "error: `{0}` is not a string in your config".format(str(input_text))
    if prev_posts and not prev_post:
        prev_post = prev_posts[0]
    if prev_posts and "{summary table}" in input_text:
        response_lines = ["\n\n|ID|Time|Author|Title|Status|\n"
                          "|:---|:-------|:------|:-----------|:------|\n"]
        for post in prev_posts:
            response_lines.append(
                "|{5}|{0}|[{1}](/u/{1})|[{2}]({3})|{4}|\n".format(post.time_utc, post.author, post.title,
                                                                  post.get_comments_url(),
                                                                  post.get_status(), post.id))
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
    response = populate_tags(f"{comment_template}{RESPONSE_TAIL}{ids}",
                             recent_post, tr_sub=subreddit, prev_post=prev_submission)
    try:
        comment = recent_post.reply(response, distinguish=distinguish, approve=approve, lock_thread=lock_thread)
        if stickied:
            comment.mod.distinguish(sticky=True)
    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden) as e:
        logger.warning('something went wrong in creating comment %s', str(e))
    return comment


def send_modmail(subreddit: TrackedSubreddit, comment_template: str,
                 recent_post: SubmittedPost = None, prev_post: SubmittedPost = None):
    response = populate_tags(comment_template, recent_post, tr_sub=subreddit, prev_post=prev_post)
    try:
        REDDIT_CLIENT.subreddit(subreddit.subreddit_name).message('modhelpfulbot', response)
    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden, AttributeError):
        logger.warning('something went wrong in sending modmail')


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


def handle_dm_command(subreddit_name: str, requestor_name, command, parameters) -> (str, bool):
    subreddit_name: str = subreddit_name[2:] if subreddit_name.startswith('r/') else subreddit_name
    subreddit_name: str = subreddit_name[3:] if subreddit_name.startswith('/r/') else subreddit_name
    command: str = command[1:] if command.startswith("$") else command

    tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name, create_if_not_exist=False)
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
        reply = populate_tags(tr_sub.canned_responses[parameters[0]], None, tr_sub=tr_sub)
        internal = False if command == "citerule" else True
        return reply, internal
    elif command == "reset":
        author_name_to_check = parameters[0] if parameters else None
        subreddit_author: SubAuthor = s.query(SubAuthor).get((tr_sub.subreddit_name, author_name_to_check))
        if subreddit_author and subreddit_author.next_eligible.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc):
            subreddit_author.next_eligible = datetime(2019, 1, 1, 0, 0)
            return "User was removed from blacklist", False
        posts = s.query(SubmittedPost).filter(SubmittedPost.author == author_name_to_check,
                                              SubmittedPost.flagged_duplicate.is_(True),
                                              SubmittedPost.subreddit_name == tr_sub.subreddit_name).all()
        for post in posts:
            post.flagged_duplicate = False
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
            return "Unban failed, I don't have permission to do that", True
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
        REDDIT_CLIENT.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
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
            subreddit_name = message.subject.lower().replace("re: ", "")
            response, _ = handle_dm_command(subreddit_name, requestor_name, command, body_parts[1:])
            message.reply(response[:9999])
            bot_owner_message = "subreddit: {0}\n\nrequestor: {1}\n\nreport: {2}\n\nreport: {3}" \
                .format(subreddit_name, requestor_name, command, response)
            REDDIT_CLIENT.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
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
        convo.read()
        return
    response = None
    response_internal = False
    command = "no_command"

    # If this conversation only has one message -> canned response or summary table
    if convo.num_messages == 1 \
            and initiating_author_name not in tr_sub.subreddit_mods \
            and initiating_author_name != "AutoModerator":

        subreddit_author: SubAuthor = s.query(SubAuthor).get((subreddit_name, initiating_author_name))
        if subreddit_author and subreddit_author.currently_blacklisted:
            convo.reply("This author is modmail-blacklisted", internal=True)
            convo.archive()

        # Automated approvals
        if tr_sub.modmail_auto_approve_messages_with_links:
            import re
            urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
            if len(urls) == 2:  # both link and link description
                print(f"found url: {urls[0][1]}")
                submission = REDDIT_CLIENT.submission(urls[0][1])
                try:
                    in_submission_urls = re.findall(LINK_REGEX, submission.selftext)
                    bad_words = "Raya", 'raya', 'dating app'
                    if not in_submission_urls and 'http' not in submission.selftext \
                            and submission.banned_by == "AutoModerator" \
                            and not any(bad_word in submission.selftext for bad_word in bad_words):
                        submission.mod.approve()
                        response = "Since you contacted the mods this bot " \
                                   "has approved your post on a preliminary basis. " \
                                   " The subreddit moderators may override this decision, however\n\n Your text:\n\n>" \
                                   + submission.selftext.replace("\n\n", "\n\n>")
                except prawcore.exceptions.NotFound:
                    pass
        if not response:
            # Create a canned reply (All reply)
            if tr_sub.modmail_all_reply:
                response = populate_tags(tr_sub.modmail_all_reply, None, tr_sub=tr_sub)
            else:
                # check if any links
                if tr_sub.modmail_no_link_reply:
                    import re
                    urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
                    if len(urls) < 2:  # both link and link description
                        response = tr_sub.modmail_no_link_reply
                else:
                    # No posts reply
                    recent_posts = s.query(SubmittedPost) \
                        .filter(SubmittedPost.subreddit_name.ilike(subreddit_name)) \
                        .filter(SubmittedPost.author == initiating_author_name).all()
                    # check if there were any missed
                    if not recent_posts:
                        check_spam_submissions(subreddit_name)
                        recent_posts = s.query(SubmittedPost) \
                            .filter(SubmittedPost.subreddit_name.ilike(subreddit_name)) \
                            .filter(SubmittedPost.author == initiating_author_name).all()
                    if recent_posts:
                        if tr_sub.modmail_posts_reply and tr_sub.modmail_posts_reply is not True:
                            response = populate_tags(tr_sub.modmail_posts_reply, None, prev_posts=recent_posts)
                            response_internal = False
                        elif tr_sub.modmail_posts_reply:
                            response = ">" + convo.messages[0].body_markdown.replace("\n\n", "\n\n>")
                            response += populate_tags(
                                "\n\n{summary table}\n\n**Please don't forget to change to 'reply as the "
                                "subreddit' below!!**\n\n"
                                "Available Commands: $summary `username`, $update, $hallpass `username`, "
                                "$unban `username`, $approve `postid`, $remove `postid`, $citerule `#`, "
                                "$blacklist `username` (modmail blacklist)"
                                "\n\nPlease subscribe to /r/ModeratelyHelpfulBot for updates", None,
                                prev_posts=recent_posts)
                            response_internal = True
                    elif tr_sub.modmail_no_posts_reply:
                        response = ">" + convo.messages[0].body_markdown.replace("\n\n", "\n\n>") + "\n\n\n\n"
                        response += populate_tags(tr_sub.modmail_no_posts_reply, None, tr_sub=tr_sub)
                        response_internal = tr_sub.modmail_no_posts_reply_internal
    else:
        last_author_name: str = convo.messages[-1].author.name
        last_message = convo.messages[-1]
        body_parts: List[str] = last_message.body_markdown.split(' ')
        command: str = body_parts[0].lower() if len(body_parts) > 0 else None
        if last_author_name != BOT_NAME and last_author_name in tr_sub.subreddit_mods:
            # check if forgot to reply as the subreddit
            if command.startswith("$") or command in ('summary', 'update'):
                response, response_internal = handle_dm_command(subreddit_name, last_author_name, command,
                                                                body_parts[1:])
            elif convo.num_messages > 2 and convo.messages[-2].author.name == BOT_NAME and last_message.is_internal:
                if not check_actioned(f"ic-{convo.id}") and tr_sub.modmail_notify_replied_internal:
                    response = "Hey sorry to bug you, but was this last message not meant to be moderator-only?  " \
                               "Just checking!  \n\n" \
                               "Set `modmail_notify_replied_internal` to False to disable this message"

                    response_internal = True
                    record_actioned(f"ic-{convo.id}")
    if response:
        try:
            convo.reply(response[0:9999], internal=response_internal)

            bot_owner_message = "subreddit: {0}\n\nrequestor: {1}\n\ncommand: {2}\n\nreport: {3}" \
                .format(subreddit_name, "modmail", command, response)
            if "no_command" is not command:
                REDDIT_CLIENT.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
        except prawcore.exceptions.BadRequest:
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
                tr_sub.send_modmail(
                             f"There was an error loading your {BOT_NAME} configuration: {{status}} "
                             f"\n\n https://www.reddit.com/r/{{subreddit_name}}"
                             f"/wiki/edit/{BOT_NAME}")
                record_actioned(f"wu-{subreddit_name}-{tr_sub.settings_revision_date}")
        else:
            to_update_db = True
    if to_update_db:
        s.add(tr_sub)
        s.commit()
    return tr_sub


def purge_old_records():
    purge_statement = "delete t  from RedditPost t inner join TrackedSubs7 s on t.subreddit_name = s.subreddit_name where  t.time_utc  < utc_timestamp() - INTERVAL greatest(s.min_post_interval_mins, 60*24*14) MINUTE  and t.flagged_duplicate=0 and t.pre_duplicate=0"
    _ = s.execute(purge_statement)


def purge_old_records_by_subreddit(tr_sub: TrackedSubreddit):
    print("looking for old records to purge from ", tr_sub.subreddit_name, tr_sub.min_post_interval)
    _ = s.query(SubmittedPost) \
        .filter(SubmittedPost.time_utc < datetime.now(pytz.utc).replace(tzinfo=None) - tr_sub.min_post_interval) \
        .filter(SubmittedPost.flagged_duplicate.is_(False)) \
        .filter(SubmittedPost.pre_duplicate.is_(False)) \
        .filter(SubmittedPost.subreddit_name == tr_sub.subreddit_name) \
        .delete()
    # print("purging {} old records from {}", len(to_delete), tr_sub.subreddit_name)
    # to_delete.delete()
    s.commit()


def check_new_submissions2a(query_limit=800):
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

    i = -1
    while True:
        print('start_loop')

        i += 1
        check_new_submissions2a()
        check_spam_submissions()

        start = datetime.now()

        # only do this if not too busy
        # if last_index < 30:

        look_for_rule_violations2(do_cleanup=(i % 30 == 0))  # uses a lot of resources

        if i % 75 == 0:
            purge_old_records()

        print("$$$checking rule violations took this long", datetime.now() - start)

        # update_TMBR_submissions(look_back=timedelta(days=7))
        send_broadcast_messages()
        #  do_automated_replies()  This is currently disabled!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        handle_direct_messages()
        handle_modmail_messages()


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

main_loop()
