from database import Base
from sqlalchemy import Column, Date, Integer, String


class Stats3(Base):
    __tablename__ = "Stats3"

    subreddit_name = Column(String(191), primary_key=True)
    author_name = Column(String(21), nullable=True, primary_key=True)
    date = Column(Date, nullable=False, primary_key=True)
    stat_name = Column(String(20), primary_key=True)
    value_int = Column(Integer)
    info = Column(String(191))

    def __init__(self, subreddit_name, author_name, date, stat_name):
        self.subreddit_name = subreddit_name
        self.author_name = author_name
        self.date = date
        self.stat_name = stat_name
        self.value_int = None
