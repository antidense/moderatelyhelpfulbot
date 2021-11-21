from pydantic.dataclasses import dataclass
from models.settings import ModMail
from models.settings import PostRestriction


@dataclass
class BaseSettings:
    post_restriction: PostRestriction
    modmail: ModMail
