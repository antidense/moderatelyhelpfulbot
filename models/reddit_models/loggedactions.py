from datetime import datetime

from core import dbobj
from sqlalchemy import Column, DateTime, String, Boolean, UnicodeText, Integer


class LoggedAction(dbobj.Base):

    __tablename__ = 'LoggedActions'
    subreddit_name = Column(String(191), nullable=True)
    action_type = Column(String(191), nullable=False, primary_key=True)
    action_id = Column(String(191), nullable=True, primary_key=True)
    date_actioned = Column(DateTime, nullable=True)
    action_try_count = Column(Integer, nullable=False)
    action_completed = Column(Boolean, nullable=True)
    error_report = Column(UnicodeText, nullable=True)

    def __init__(self, subreddit_name, action_type, action_id):
        self.action_type = action_type
        self.action_id = action_id
        self.subreddit_name = subreddit_name
        self.date_actioned = datetime.now()
        self.action_try_count = 0
        self.action_completed = False
        self.is_new = True


def open_logged_action(wd, subreddit_name, action_type, action_id):
    logged_action = wd.s.query(LoggedAction).get({"action_type": action_type, "action_id": action_id})
    if logged_action:
        logged_action.is_new = False
        return logged_action
    else:
        print(f"{subreddit_name} {action_type} {action_id}")
        logged_action = LoggedAction(subreddit_name, action_type, action_id)
        wd.s.add(logged_action)
        logged_action.action_try_count += 1

        return logged_action
