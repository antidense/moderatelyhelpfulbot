from datetime import datetime

import praw
import prawcore
import pytz
from core import dbobj
from logger import logger
from praw.models import Submission
from sqlalchemy import Boolean, Column, DateTime, Integer, String, UnicodeText
from enums import CountedStatus, PostedStatus

# from models.reddit_models.redditinterface import SubmissionInfo

s = dbobj.s



class SubmittedPost(dbobj.Base):  # need posted_status
    __tablename__ = 'RedditPost'
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    author = Column(String(21), nullable=True)
    submission_text = Column(String(191), nullable=True)
    time_utc = Column(DateTime, nullable=False)
    subreddit_name = Column(String(21), nullable=True)
    # banned_by = Column(String(21), nullable=True)  # Can delete this now ---------
    flagged_duplicate = Column(Boolean, nullable=True)
    pre_duplicate = Column(Boolean, nullable=True)  # Redundant with "CountedStatus.COUNTS
    # self_deleted = Column(Boolean, nullable=True)  # Can delete this now -------
    reviewed = Column(Boolean, nullable=True)   # could be combined into an Enum: reviewed,  pre_duplicate
    last_checked = Column(DateTime, nullable=False)  # redundant with reviewed?  can't use as inited with old date
    bot_comment_id = Column(String(10), nullable=True)  # wasn't using before but using now
    is_self = Column(Boolean, nullable=True)  # Can delete this now----------
    # removed_status = Column(String(21), nullable=True)  # not used
    post_flair = Column(String(191), nullable=True)
    author_flair = Column(String(191), nullable=True)
    # author_css = Column(String(191), nullable=True)
    counted_status = Column(Integer)
    # newly added
    response_time = Column(DateTime, nullable=True)  # need to add everywhere
    review_debug = Column(UnicodeText, nullable=True)
    flushed_to_log = Column(Boolean, nullable=False)
    nsfw_repliers_checked = Column(Boolean, nullable=False)  # REMOVE!!
    nsfw_last_checked = Column(DateTime, nullable=True)

    added_time = Column(DateTime, nullable=True)  # added 6/30/22
    posted_status = Column(String(30), nullable=False)
    banned_by = Column(String(21), nullable=True)
    is_oc = Column(Boolean, nullable=False)

    reply_comment = Column(UnicodeText, nullable=True)
    last_reviewed = Column(DateTime, nullable=False)

    api_handle = None

    def __init__(self, submission, save_text: bool = False):
        self.nsfw_last_checked = datetime.now(pytz.utc)
        self.last_checked = datetime.now(pytz.utc)
        self.flushed_to_log = False
        if isinstance(submission, Submission):

            self.id = submission.id
            self.title = submission.title[0:190]
            self.author = str(submission.author)
            if save_text:
                self.submission_text = submission.selftext[0:190]
            self.time_utc = datetime.utcfromtimestamp(submission.created_utc)
            self.subreddit_name = str(submission.subreddit).lower()
            self.added_time = datetime.now(pytz.utc)
            self.last_reviewed = datetime.now(pytz.utc)
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
            # self.author_css = submission.author_flair_css_class
            self.response_time = None
            self.nsfw_last_checked = self.time_utc
            self.nsfw_repliers_checked = False
            self.posted_status = PostedStatus.UNKNOWN.value
            self.is_oc = submission.is_original_content
        else:
            subm_info = submission

            self.id = subm_info.id
            self.title = subm_info.title
            self.submission_text = None
            self.subreddit_name = subm_info.subreddit_name
            self.added_time = datetime.now(pytz.utc)
            self.last_reviewed = datetime.now(pytz.utc)
            self.flagged_duplicate = False
            self.reviewed = False
            self.banned_by = subm_info.banned_by
            self.api_handle = None
            self.pre_duplicate = False
            self.self_deleted = False
            self.is_self = subm_info.is_self
            self.counted_status = -1  # CountedStatus.NOT_CHKD.value
            self.post_flair = subm_info.post_flair
            self.author_flair = subm_info.author_flair
            # self.author_css = subm_info.author_css
            self.response_time = None
            self.nsfw_last_checked = self.time_utc
            self.nsfw_repliers_checked = False
            self.posted_status = PostedStatus.UNKNOWN.value
            self.is_oc = subm_info.is_oc

    def get_url(self) -> str:
        return f"http://redd.it/{self.id}"

    def get_comments_url(self) -> str:
        return f"https://www.reddit.com/r/{self.subreddit_name}/comments/{self.id}"

    def get_removed_explanation_url(self):
        if not self.bot_comment_id:
            return None
        return f"https://www.reddit.com/r/{self.subreddit_name}/comments/{self.id}//{self.bot_comment_id}"


    def update_status(self, reviewed=None, flagged_duplicate=None, counted_status=None):
        if reviewed is not None:
            self.reviewed = reviewed

        if counted_status is not None and counted_status != CountedStatus.REMOVED:
            self.counted_status = counted_status.value
        if flagged_duplicate is not None:
            self.flagged_duplicate = flagged_duplicate
            if flagged_duplicate is True:
                self.counted_status = CountedStatus.FLAGGED.value
        self.last_checked = datetime.now(pytz.utc)
        # self.response_time = datetime.now(pytz.utc)-self.time_utc.replace(tzinfo=timezone.utc)
        if not self.response_time:
            self.response_time = datetime.now(pytz.utc)
