from enum import Enum


class SubStatus(Enum):
    UNKNOWN = 20
    ACTIVE = 10
    YAML_SYNTAX_OKAY = 8
    NO_BAN_ACCESS = 4
    CONFIG_ERROR = 3
    NO_CONFIG = 2
    CONFIG_ACCESS_ERROR = 1
    NOT_MOD = 0
    SUB_GONE = -1
    SUB_FORBIDDEN = -2
