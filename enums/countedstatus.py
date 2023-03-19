from enum import Enum

class CountedStatus(Enum):
    NEEDS_UPDATE = -2  # may be exempt but don't have that information (is automoderator or mod-removed exempt?)
    NOT_CHKD = -1   # include in search
    PREV_EXEMPT = 0  # Previously the code for exemption, switched to 2
    COUNTS = 1  # include in search
    # EXEMPTED = 2  # don't include in search  0 --> CHANGE to 2*****  no longer use, use more specific
    REVIEWED = 2 # include in search
    BLKLIST = 3  # don't include in search
    HALLPASS = 4  # don't include in search
    FLAGGED = 5 # don't include?
    SPAMMED_EXMPT = 6  #<<< doesn't exist???
    AM_RM_EXEMPT = 7
    MOD_RM_EXEMPT = 8
    OC_EXEMPT = 9
    SELF_EXEMPT = 10
    LINK_EXEMPT = 11
    FLAIR_EXEMPT = 12
    FLAIR_NOT_EXEMPT = 13
    TITLE_KW_EXEMPT = 14
    TITLE_CRITERIA_NOT_MET = 15
    MODPOST_EXEMPT = 16
    GRACE_PERIOD_EXEMPT = 17
    FLAIR_HELPER = 18
    REMOVED = 20
    SUB_CONFIG_ERROR = 25
    BOT_SPAM = 30
    AGED_OUT = 40
    REMOVE_FAILED = 50
    NOT_TO_REMOVE = 90
    BLKLIST_NEED_REMOVE = 503
    NEED_REMOVE = 520
    BLKLIST_REMOVED_FAILED = 603

    @staticmethod
    def is_permanent(cs):
        if cs in (CountedStatus.COUNTS, CountedStatus.BLKLIST, CountedStatus.HALLPASS, CountedStatus.FLAGGED,
                  CountedStatus.AM_RM_EXEMPT,):  # need to finish
            return True
        else:
            return False
