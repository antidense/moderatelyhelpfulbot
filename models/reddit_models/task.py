


class Task(Base):
    __tablename__ = 'aa_tasks'

    task_name = Column(String(191), nullable=False, primary_key=True)
    run_interval = Column(Integer, nullable=False)
    last_ran = Column(DateTime, nullable=False)
    last_error = Column(Text, nullable=True)
    last_report = Column(Text, nullable=True)
    force_run = Column(Boolean, nullable=False)

    def __init__(self, task_name, run_interval):
        self.task_name = task_name
        self.run_interval = run_interval
        self.last_ran = None
        self.last_error = None
        self.last_report = None
        self.force_run = False

## Hall Pass used notification: author, post, subreddit


"""
    task_name = Column(String(191), nullable=False, primary_key=True)
    run_interval = Column(Integer, nullable=False)
    last_ran = Column(DateTime, nullable=False)
    last_error = Column(Text, nullable=True)
    last_report = Column(Text, nullable=True)
    force_run = Column(Boolean, nullable=False)

    def __init__(self, task_name, run_interval):
        self.task_name = task_name
        self.run_interval = run_interval
        self.last_ran = None
        self.last_error = None
        self.last_report = None
        self.force_run = False

"""

