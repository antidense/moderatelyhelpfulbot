from typing import Union
from pydantic.dataclasses import dataclass


@dataclass
class MainSettings:
    max_count_per_interval: int
    ignore_AutoModerator_removed: bool  # pylint: disable=invalid-name
    ignore_moderator_removed: bool
    ban_threshold_count: int
    notify_about_spammers: Union[bool, int]
    ban_duration_days: int
