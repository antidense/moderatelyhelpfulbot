from datetime import datetime

from core import dbobj
from praw.models import Submission
from sqlalchemy import Column, DateTime, String

class CommonPost(dbobj.Base):
    __tablename__ = 'CommonPosts'
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


