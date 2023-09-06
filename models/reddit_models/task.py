from datetime import datetime, timedelta
import prawcore
from core import dbobj
from sqlalchemy import Boolean, Column, DateTime, Integer, String, UnicodeText
from logger import logger as log

class Task(dbobj.Base):
    __tablename__ = 'Tasks2'
    wd = None
    task_name = Column(String(191), nullable=True, primary_key=False)
    target_function = Column(String(191), nullable=False, primary_key=True)
    last_run_dt = Column(DateTime, nullable=True)
    frequency_secs = Column(Integer, nullable=False)
    max_duration_secs = Column(Integer, nullable=True)
    last_error = Column(UnicodeText, nullable=True)
    # time out?

    #target_function = None
    # last_run_dt = None
    # frequency_secs = timedelta(minutes=5)
    # max_duration = timedelta(minutes=5)
    task_durations = []
    error_count = 0
    # last_error = ""

    def __init__(self, wd,  target_function,  frequency: timedelta):
        # self.task_name=task_name
        self.wd = wd
        self.target_function = target_function
        # self.frequency : timedelta= frequency
        self.frequency_secs = frequency.total_seconds()
        self.max_duration_secs = 0
        self.last_run_dt = None
        self.last_error = None



        """
        except (prawcore.exceptions.ServerError, prawcore.exceptions.ResponseException) as e:
            import time
            print("sleeping due to server error")
            import traceback
            print(traceback.format_exc())
            time.sleep(60 * 5)  # sleep for a bit server errors

            import traceback
            trace = traceback.format_exc()
            print(trace)
            # wd.ri.send_modmail(subreddit_name=BOT_NAME, subject="[Notification] MHB Exception", body=trace,
            #                    use_same_thread=True)
            #wd.s.add(wd.ri.bot_sub)
            #wd.s.commit()
        """
