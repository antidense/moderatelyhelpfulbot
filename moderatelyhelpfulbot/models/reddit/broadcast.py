from database import Base
from sqlalchemy import Boolean, Column, String

# For messaging subreddits that use bot


class Broadcast(Base):
    __tablename__ = "Broadcast"
    id = Column(String(10), nullable=True, primary_key=True)
    title = Column(String(191), nullable=True)
    text = Column(String(191), nullable=True)
    subreddit = Column(String(191), nullable=True)
    sent = Column(Boolean, nullable=True)

    def __init__(self, post):
        self.id = post.id
