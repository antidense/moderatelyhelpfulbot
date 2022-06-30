from datetime import datetime

from core import dbobj
from sqlalchemy import Boolean, Column, DateTime, Integer, String, UnicodeText

class SubAuthor(dbobj.Base):
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

