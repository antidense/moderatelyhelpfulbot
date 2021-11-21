from datetime import datetime

from database import Base
from sqlalchemy import Column, DateTime, String


class LoggedActions(Base):
    __tablename__ = "Actions"
    subreddit_name = Column(String(21), nullable=True)
    action_id = Column(String(30), nullable=True, primary_key=True)
    date_actioned = Column(DateTime, nullable=True)

    def __init__(
        self,
        comment_id,
    ):
        self.comment_id = comment_id
        self.date_actioned = datetime.now()
