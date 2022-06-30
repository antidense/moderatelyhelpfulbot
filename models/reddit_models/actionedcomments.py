from datetime import datetime
from core import dbobj
from sqlalchemy import Column, DateTime, String


class ActionedComments(dbobj.Base):
    __tablename__ = 'ActionedComments'
    comment_id = Column(String(30), nullable=True, primary_key=True)
    date_actioned = Column(DateTime, nullable=True)

    # TODO
    # add subreddit name
    # add success or fail

    def __init__(self, comment_id, ):
        self.comment_id = comment_id
        self.date_actioned = datetime.now()
