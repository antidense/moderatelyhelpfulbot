from settings import settings
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine(settings["database"]["engine"])
Base = declarative_base(bind=engine)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
s = Session()
s.rollback()


def get_session():
    return s
