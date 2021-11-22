from typing import Literal, Optional
from pydantic import validator
from pydantic.dataclasses import dataclass


@dataclass
class PostRestriction:
    action: Optional[
        Literal[
            "remove",
            "modmail",
            "comment",
            "grace_period_mins",
            "report"
        ]
    ] = None
    approve: Optional[bool] = None
    author_exempt_flair_keyword: Optional[str] = None
    ban_duration_days: Optional[int] = None
    ban_threshold_count: Optional[int] = None
    comment: Optional[str] = None
    distinguish: Optional[bool] = None
    exempt_link_posts: Optional[bool] = None
    exempt_self_posts: Optional[bool] = None
    grace_period_mins: Optional[int] = None
    ignore_autoModerator_removed: Optional[bool] = None
    ignore_moderator_removed: Optional[bool] = None
    lock_thread: Optional[bool] = None
    max_count_per_interval: Optional[int] = None
    min_post_interval_hrs: Optional[int] = None
    notify_about_spammers: Optional[bool] = None
    report_reason: Optional[str] = None
    title_exempt_keyword: Optional[str] = None

    @validator('ban_duration_days')
    # pylint: disable=no-self-argument
    def ban_duration_days_valid(cls, value):
        # pylint: enable=no-self-argument
        if value and (value < 0 or value > 999):
            raise ValueError("Ban duration must be 0, above 0 and below 999")
        return value
