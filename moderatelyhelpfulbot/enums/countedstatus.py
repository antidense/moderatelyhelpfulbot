from enum import Enum


class CountedStatus(Enum):
    NOT_CHKD = -1  # include in search
    PREV_EXEMPT = 0  # Previously the code for exemption, switched to 2
    COUNTS = 1  # include in search

    # don't include in search  0 --> CHANGE to 2***** no longer use,
    # use more specific
    EXEMPTED = 2
    BLKLIST = 3  # don't include in search
    HALLPASS = 4  # don't include in search
    FLAGGED = 5
    SPAMMED_EXMPT = 6
    AM_RM_EXEMPT = 7
    MOD_RM_EXEMPT = 8
    OC_EXEMPT = 9
    SELF_EXEMPT = 10
    LINK_EXEMPT = 11
    FLAIR_EXEMPT = 12
    FLAIR_NOT_EXEMPT = 13
    TITLE_KW_EXEMPT = 14
    TITLE_KW_NOT_EXEMPT = 15
    MODPOST_EXEMPT = 16
    GRACE_PERIOD_EXEMPT = 17
    FLAIR_HELPER = 18
    REMOVED = 20
    BOT_SPAM = 30
