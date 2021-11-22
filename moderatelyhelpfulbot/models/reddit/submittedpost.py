from datetime import datetime, timedelta, timezone

import praw
import prawcore
import pytz
from settings import settings
from database import Base, get_session
from enums import CountedStatus, PostedStatus
from logger import logger
from praw.models import Submission
from reddit import REDDIT_CLIENT
from sqlalchemy import Boolean, Column, DateTime, Integer, String
from utils import get_age

s = get_session()


BOT_NAME = settings["bot_name"]


class SubmittedPost(Base):
    __tablename__ = "RedditPost"
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    author = Column(String(21), nullable=True)
    submission_text = Column(String(191), nullable=True)
    time_utc = Column(DateTime, nullable=False)
    subreddit_name = Column(String(21), nullable=True)
    # banned_by = Column(String(21), nullable=True)  # Can delete this now ----
    flagged_duplicate = Column(Boolean, nullable=True)
    pre_duplicate = Column(Boolean, nullable=True)
    # self_deleted = Column(Boolean, nullable=True)  # Can delete this now ----
    reviewed = Column(
        Boolean, nullable=True
    )  # could be combined into an Enum: reviewed,  pre_duplicate
    last_checked = Column(
        DateTime, nullable=False
    )  # redundant with reviewed?  can't use as inited with old date
    bot_comment_id = Column(
        String(10), nullable=True
    )  # wasn't using before but using now
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
        if self.subreddit_name == "needafriend":
            if 25 > self.age > 12:
                self.post_flair = "strict sfw"

                author: TrackedAuthor = s.query(TrackedAuthor).get(self.author)
                if not author:
                    author = TrackedAuthor(self.author)
                if author:
                    if (
                        author.nsfw_pct == -1
                        or not author.last_calculated
                        or author.last_calculated.replace(tzinfo=timezone.utc)
                        < (datetime.now(pytz.utc) - timedelta(days=7))
                    ):
                        nsfw_pct, items = author.calculate_nsfw()
                        new_flair_text = f"{int(nsfw_pct)}% NSFW"
                        s.add(author)

                        if nsfw_pct > 60:
                            try:
                                REDDIT_CLIENT \
                                    .subreddit(self.subreddit_name) \
                                    .flair.set(
                                        self.author, text=new_flair_text
                                    )
                            except (
                                praw.exceptions.APIException,
                                prawcore.exceptions.Forbidden,
                            ):
                                pass

    def get_url(self) -> str:
        return f"http://redd.it/{self.id}"

    def get_comments_url(self) -> str:
        return (
            f"https://www.reddit.com/r/{self.subreddit_name}"
            f"/comments/{self.id}"
        )

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
        return (
            f"https://www.reddit.com/r/{self.subreddit_name}/comments/"
            f"{self.id}//{self.bot_comment_id}"
        )

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
            link = f"http://redd.it/{self.id}"
            logger.warning(
                f"something went wrong removing, post: {link}"
            )
            return False
        except (
            prawcore.exceptions.Forbidden,
            prawcore.exceptions.ServerError
        ):
            link = f"http://redd.it/{self.id}"
            logger.warning(
                f"I was not allowed to remove the post: {link}"
            )
            return False

    def reply(
        self,
        response,
        distinguish=True,
        approve=False,
        lock_thread=True
    ):
        try:
            # first try to lock thread -
            # useless to make a comment unless it's possible
            if lock_thread:
                self.get_api_handle().mod.lock()
            comment = self.get_api_handle().reply(response)
            if distinguish:
                comment.mod.distinguish()
            if approve:
                comment.mod.approve()
            return comment
        except praw.exceptions.APIException:
            link = f"http://redd.it/{self.id}"
            logger.warning(
                f"Something went wrong with replying to this post: {link}"
            )
            return False
        except (
            prawcore.exceptions.Forbidden,
            prawcore.exceptions.ServerError
        ):
            link = f"http://redd.it/{self.id}"
            logger.warning(
                f"Something with replying to this post:: {link}"
            )
            return False
        except (prawcore.exceptions.BadRequest):
            link = f"http://redd.it/{self.id}"
            logger.warning(
                f"Something with replying to this post:: {link}"
            )
            return False

    def get_posted_status(self, get_removed_info=False) -> PostedStatus:
        _ = self.get_api_handle()
        try:
            self.self_deleted = False if self.api_handle and \
                self.api_handle.author else True
        except prawcore.exceptions.Forbidden:
            return PostedStatus.UNKNOWN
        self.banned_by = self.api_handle.banned_by
        if not self.banned_by and not self.self_deleted:
            return PostedStatus.UP
        elif self.banned_by:
            if self.banned_by is True:
                return PostedStatus.SPAM_FLT
            if (
                not self.bot_comment_id and get_removed_info
            ):  # make sure to commit to db
                top_level_comments = list(self.get_api_handle().comments)
                for c in top_level_comments:
                    if (
                        hasattr(c, "author")
                        and c.author
                        and c.author.name == self.banned_by
                    ):
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

    def update_status(
        self,
        reviewed=None,
        flagged_duplicate=None,
        counted_status=None
    ):
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
