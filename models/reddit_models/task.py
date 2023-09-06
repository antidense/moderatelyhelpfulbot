
class Task(dbobj.Base):

    __tablename__ = 'Tasks'
    wd = None
    target_function = Column(String(191), nullable=False, primary_key=True)
    last_run_dt = Column(DateTime, nullable=True)
    frequency_secs = Column(Integer, nullable=False)
    max_duration_secs = Column(Integer, nullable=True)
    last_error = Column(Text, nullable=True)
    # time out?

    target_function = None
    last_run_dt = None
    # frequency_secs = timedelta(minutes=5)
    # max_duration = timedelta(minutes=5)
    task_durations = []
    error_count = 0
    last_error = ""

    def __init__(self, wd, target_function,  frequency: timedelta):
        self.wd = wd
        self.target_function = target_function
        # self.frequency : timedelta= frequency
        self.frequency_sec s= frequency.total_seconds()
        self.max_duration_secs = 0

    def run_task(self):

        if self.last_run_dt and self.last_run_dt + timedelta(seconds=self.frequency) > datetime.now():
            log.debug(f"Skipping task as not due for task: {self.target_function}")
            pass
        elif self.error_count > 5 and self.last_run_dt + timedelta(hours=5) > datetime.now():
            # if had multiple erros  and last ran less than five hours ago
            log.debug(f"Skipping task due to previous errors: {self.target_function} {self.last_error}")
        else:
            start_time = datetime.now()
            try:
                log.debug(f"Running task: {self.target_function}, last ran:{self.last_run_dt}")
                globals()[self.target_function](self.wd)
                end_time = datetime.now()
                self.last_run_dt = start_time
                log.debug(f"Task complete {self.target_function} {end_tim e -start_time}")
                self.task_durations.append((end_tim e -start_time).seconds)
            except (prawcore.exceptions.ServerError, prawcore.exceptions.ResponseException):
                self.wd.s.commit()
                self.error_count += 1
                import traceback
                trace = traceback.format_exc()
                print(trace)
                return -1
            except Exception:
                self.wd.s.commit()
                import traceback
                trace = traceback.format_exc()
                self.last_error = str(trace)
                print(trace)

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
