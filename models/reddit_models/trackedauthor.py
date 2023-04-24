
from collections import Counter
from datetime import datetime
from typing import List

import praw
import prawcore
import pytz
import re
from core import dbobj, ASL_REGEX
from praw.models import ListingGenerator, Submission
from praw.models.listing.mixins.redditor import SubListing
from sqlalchemy import Column, DateTime, Integer, String, UnicodeText


def get_age(input_text):
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


class TrackedAuthor(dbobj.Base):
    __tablename__ = 'TrackedAuthor'
    author_name = Column(String(21), nullable=True, primary_key=True)
    nsfw_pct = Column(Integer)
    last_calculated = Column(DateTime, nullable=True)
    created_date = Column(DateTime, nullable=False)
    gender = Column(String(5), nullable=True)
    num_history_items = Column(Integer)
    has_nsfw_post = Column(String(10), nullable=True)
    sub_counts = Column(UnicodeText, nullable=True)
    sub_messages = Column(UnicodeText, nullable=True)
    bad_posts = Column(UnicodeText, nullable=True)
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



    def calculate_nsfw(self, wd, instaban_subs=None):
        if self.author_name and  self.author_name.lower() == "automoderator":
            self.nsfw_pct=0

            return 0,0
        subs = list()
        intended_total = 50
        total = 0
        count = 0
        age = 0

        api_handle: praw.models.Redditor = wd.ri.reddit_client.redditor(self.author_name)
        sub_listing: SubListing = api_handle.comments
        comments_generator: ListingGenerator = sub_listing.new(limit=intended_total)
        post_listing: SubListing = api_handle.submissions
        post_generator: ListingGenerator = post_listing.new(limit=10)
        bad_messages = []
        bad_posts = []
        try:
            comments: List[praw.models.reddit.comment.Comment] = list(comments_generator)
            for comment in comments:
                assert isinstance(comment, praw.models.reddit.comment.Comment)
                subreddit_name = comment.subreddit.display_name
                # ignore  posts made in author's own subreddit
                if self.author_name in subreddit_name:
                    continue
                subs.append(subreddit_name)
                total += 1
                if comment.subreddit.over18:
                    count += 1
                    bad_messages.append(comment.id)
                if not age:
                    age = get_age(comment.body)
                    if age > 10:
                        self.age = age
            posts: List[praw.models.reddit.Submission.submission] = list(post_generator)
            for post in posts:
                subreddit_name = post.subreddit.display_name
                subs.append(subreddit_name)
                # ignore  posts made in author's own subreddit
                if self.author_name in subreddit_name:
                    continue
                total += 1
                if post.over_18:
                    count += 1
                    if not post.is_self:
                        self.has_nsfw_post = post.id
                    bad_posts.append(post.id)
                if not age:
                    age = get_age(post.title)
                    if age > 10:
                        self.age = age
            self.nsfw_pct = None if total == 0 else (count*100)/total
        except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden):
            pass
        self.age = age
        self.last_calculated = datetime.now(pytz.utc)
        self.num_history_items = total

        if instaban_subs:
            for sub in subs:
                # print("instaban_subs", instaban_subs)
                if sub in instaban_subs:
                    self.has_banned_subs_activity = True
                    break
        from collections import Counter
        self.sub_counts = str(Counter(subs))
        self.sub_messages = str(bad_messages)
        self.bad_posts = str(bad_posts)
        return self.nsfw_pct, total

        # In the future,  look for: /r/dirtypenpals   DDLGPersonals   cglpersonals  littlespace dirtyr4r ddlg
        # instantban: rapefantasies, womenarethings, HypnoHookup, MisogynyGoneWild


