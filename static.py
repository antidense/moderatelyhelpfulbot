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
from enum import Enum  # has to be at the end?
import queue
from settings import DB_ENGINE
from enums import *
from static import *
# from database import Database



# Set up some global variables
LINK_REGEX = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
REDDIT_LINK_REGEX = r'r/([a-zA-Z0-9_]*)/comments/([a-z0-9_]*)/([a-zA-Z0-9_]{0,50})'
RESPONSE_TAIL = """

-------------------------------------------------

^^BOOP! ^^BLEEP! ^^I ^^am ^^a ^^bot. ^^Concerns? ^^Message ^^[/r/{subreddit}](https://www.reddit.com/message/compose?to=%2Fr%2F{subreddit}&subject=problem%20with%20bot)."""
MAIN_SETTINGS = dict()
WATCHED_SUBS = dict()
SUBWIKI_CHECK_INTERVAL_HRS = 24
UPDATE_LIST = True
ACTIVE_SUB_LIST = []
NEW_SUBMISSION_Q = queue.Queue()
SPAM_SUBMISSION_Q = queue.Queue()
DEFAULT_CONFIG = """---
###### If you edit this page, you must [click this link, then click "send"](https://old.reddit.com/message/compose?to=moderatelyhelpfulbot&subject=subredditname&message=update) to have the bot update
######https://www.reddit.com/r/moderatelyhelpfulbot/wiki/index

###### [User Summary, click this link](https://www.reddit.com/message/compose?to=moderatelyhelpfulbot&subject=subredditname&message=$summary u/username) - Will show you the users posts

###### [User Extra Post, click this link](https://www.reddit.com/message/compose?to=moderatelyhelpfulbot&subject=subredditname&message=$hallpass u/username)  - Will allow the user one extra post
post_restriction: 
    max_count_per_interval: 1
    min_post_interval_hrs: 72
    action: remove
    ban_threshold_count: 5
    ban_duration_days: ~
    comment: "Hello and thank you for posting to {subreddit}! It seems you have previously posted {maxcount} submission within {interval}, so your post has been removed as per the post frequency rule.  If you believe your post has been removed by mistake please [message the moderators](https://old.reddit.com/message/compose?to=%2Fr%2F{subreddit}).\n"
    distinguish: true
    grace_period_mins: 60
    ignore_AutoModerator_removed: true
    ignore_moderator_removed: true
    title_exempt_keyword: Modpost
modmail: 
    modmail_all_reply: ~
    modmail_no_posts_reply: "Hello, and thank you for your message. I could not find any prior posts from you. If you have a particular question about a post, please reply with a link to the post!\n"
    modmail_no_posts_reply_internal: false
    modmail_posts_reply: ~
"""


NAFMC = "Per our rules, contacting users less than 18 years old while having a history of NSFW comments and/or posts " \
        "is a bannable offense. Your account was reviewed by a mod team and determined to be non-compliant with our rules."

NAFSC = "Per recent community feedback, we are temp banning anyone with a history that is more than " \
        "80% NSFW to protect users younger than 18 and reduce sexual harassment in our subreddit.  " \
        "Please get this down if you wish to continue to participate here. " \
        "Your score is currently {NSFWPCT}% and is recalculated weekly."

NAFBS = "It appears you have content on your profile from certain subreddits that we feel are incompatible " \
        "with searching for platonic friendships. See https://www.reddit.com/r/Needafriend/wiki/banned_subs"

NAFCF = f"Per our rules, catfishing -- identifying as different ages in different posts -- is a bannable offense."

NSFW_SKIP_USERS = ["automoderator"]

ASL_REGEX = r"((?P<age>[0-9]{2})([/ \\-]?|(\]? \[))(?P<g>[mMFf]{1}))|" \
            r"((?P<g2>[mMFf]{1})([/ \\-]?|(\]? \[))(?P<age2>[0-9]{2}))"