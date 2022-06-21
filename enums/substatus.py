from enum import Enum

class SubStatus(Enum):
    UNKNOWN = 20
    ACTIVE = 10
    NO_BAN_ACCESS = 8
    MHB_CONFIG_ERROR = 5
    YAML_SYNTAX_OK = 4
    YAML_SYNTAX_ERROR = 3
    NO_CONFIG = 2
    CONFIG_ACCESS_ERROR = 1
    NO_MOD_PRIV = 0
    SUB_GONE = -1
    SUB_FORBIDDEN = -2