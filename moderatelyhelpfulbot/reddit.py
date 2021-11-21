import praw
from settings import settings

REDDIT_CLIENT = praw.Reddit(
    client_id=settings["client_id"],
    client_secret=settings["client_secret"],
    password=settings["bot_password"],
    user_agent="ModeratelyHelpfulBot v0.4",
    username=settings["bot_name"],
)
