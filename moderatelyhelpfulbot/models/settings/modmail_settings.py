

from typing import Optional
from pydantic.dataclasses import dataclass


@dataclass
class ModMail:
    modmail_all_reply: Optional[bool] = False
    modmail_auto_approve_messages_with_links: Optional[bool] = False
    modmail_no_posts_reply: Optional[str] = None
    modmail_no_posts_reply_internal: Optional[bool] = False
    modmail_posts_reply: Optional[bool] = False
    modmail_removal_reason_helper: Optional[bool] = False
