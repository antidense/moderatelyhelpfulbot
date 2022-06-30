from settings import login_credentials
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class Database:
    def __init__(self):
        self.engine = create_engine(login_credentials["database"]["engine"])
        self.Base = declarative_base(bind=self.engine)
        Session = sessionmaker(bind=self.engine)
        self.s = Session()
        self.s.rollback()

    def load_models(self):
        import models.reddit_models

        self.Base.metadata.create_all(self.engine)
        print("Loading database modules")