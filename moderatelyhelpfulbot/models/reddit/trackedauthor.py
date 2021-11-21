from datetime import datetime
from typing import List

import praw
import prawcore
import pytz
from database import Base
from praw.models import ListingGenerator
from praw.models.listing.mixins.redditor import SubListing
from reddit import REDDIT_CLIENT
from sqlalchemy import Column, DateTime, Integer, String
from utils import get_age


class TrackedAuthor(Base):
    __tablename__ = "TrackedAuthor"
    author_name = Column(String(21), nullable=True, primary_key=True)
    nsfw_pct = Column(Integer)
    last_calculated = Column(DateTime, nullable=True)
    created_date = Column(DateTime, nullable=False)
    gender = Column(String(5), nullable=True)
    num_history_items = Column(Integer)
    has_nsfw_post = Column(String(10), nullable=True)
    sub_counts = Column(String(191), nullable=True)
    age = Column(Integer)
    api_handle = None
    has_banned_subs_activity = False

    def __init__(self, author_name):
        self.author_name = author_name
        self.nsfw_pct = -1
        self.created_date = datetime.now(pytz.utc)
        self.num_history_items = -1
        self.has_nsfw_post = None
        self.sub_counts = None
        self.has_banned_subs_activity = False

    def get_api_handle(self):
        if not self.api_handle:
            self.api_handle = REDDIT_CLIENT.redditor(self.author_name)
            return self.api_handle
        else:
            return self.api_handle

    def calculate_nsfw(self, instaban_subs=None):
        subs = list()
        intended_total = 50
        total = 0
        count = 0
        age = 0
        api_handle: praw.models.Redditor = self.get_api_handle()
        sub_listing: SubListing = api_handle.comments
        comments_generator: ListingGenerator = sub_listing.new(
            limit=intended_total
        )
        post_listing: SubListing = api_handle.submissions
        post_generator: ListingGenerator = post_listing.new(limit=10)
        try:
            comments: List[praw.models.reddit.comment.Comment] = list(
                comments_generator
            )
            for comment in comments:
                assert isinstance(comment, praw.models.reddit.comment.Comment)
                subs.append(comment.subreddit.display_name)
                total += 1
                if comment.subreddit.over18:
                    count += 1
                if not age:
                    age = get_age(comment.body)
                    if age > 10:
                        self.age = age
            posts: List[praw.models.reddit.Submission.submission] = list(post_generator)
            for post in posts:
                subs.append(post.subreddit.display_name)
                total += 1
                if post.over_18:
                    count += 1
                    if not post.is_self:
                        self.has_nsfw_post = post.id
                if not age:
                    age = get_age(post.title)
                    if age > 10:
                        self.age = age
            self.nsfw_pct = None if total == 0 else (count * 100) / total
        except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden):
            pass
        self.age = age
        self.last_calculated = datetime.now(pytz.utc)
        self.num_history_items = total

        if instaban_subs:
            for sub in subs:
                print("instaban_subs", instaban_subs)
                if sub in instaban_subs:
                    self.has_banned_subs_activity = True
                    break
        from collections import Counter

        self.sub_counts = str(Counter(subs))
        return self.nsfw_pct, total

        # In the future,  look for: /r/dirtypenpals   DDLGPersonals   cglpersonals  littlespace dirtyr4r ddlg
        # instantban: rapefantasies, womenarethings, HypnoHookup, MisogynyGoneWild
