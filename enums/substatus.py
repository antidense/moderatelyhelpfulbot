from enum import Enum

class SubStatus(Enum):
    UNKNOWN = 20
    ACTIVE = 10 # recheck 3-4 days
    NO_BAN_ACCESS = 8 # recheck 3-4 days
    MHB_CONFIG_ERROR = 5 # recheck daily
    YAML_SYNTAX_OK = 4 # recheck daily
    YAML_SYNTAX_ERROR = 3 # recheck daily
    NO_CONFIG = 2 # recheck daily
    CONFIG_ACCESS_ERROR = 1 # recheck daily
    NO_MOD_PRIV = 0 # recheck daily
    SUB_GONE = -1  # never recheck
    SUB_FORBIDDEN = -2   # never recheck

#☑ sub exists(0); ☑ mod access(1); ☑ wiki access (2); ☑ found config file(3); ☑ config syntax ok(5); ☑ config ok(6); ☑ ban access (9)