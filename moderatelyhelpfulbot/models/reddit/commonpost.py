from datetime import datetime

from database import Base
from praw.models import Submission
from reddit import REDDIT_CLIENT
from sqlalchemy import Column, DateTime, String


class CommonPost(Base):
    __tablename__ = "CommonPosts"
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
        return (
            f"https://www.reddit.com/r/"
            f"{self.subreddit_name}/comments/{self.id}"
        )

    def get_api_handle(self) -> Submission:
        if not self.api_handle:
            self.api_handle = REDDIT_CLIENT.submission(id=self.id)
            return self.api_handle
        else:
            return self.api_handle
