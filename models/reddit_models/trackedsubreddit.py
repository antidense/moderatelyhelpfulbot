import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import humanize
import pytz
import yaml
from core import dbobj
from enums import CountedStatus, SubStatus
from models.reddit_models import SubmittedPost
from logger import logger
from sqlalchemy import (
    SMALLINT,
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    UnicodeText,
    desc,
    func
)

from settings import MAIN_BOT_NAME
from sqlalchemy import Enum

s = dbobj.s


class TrackedSubreddit(dbobj.Base):
    __tablename__ = 'TrackedSubs'
    subreddit_name = Column(String(21), nullable=False, primary_key=True)
    # checking_mail_enabled = Column(Boolean, nullable=True)  #don't need this?
    settings_yaml_txt = Column(UnicodeText, nullable=True)
    settings_yaml = None
    last_updated = Column(DateTime, nullable=True)
    # last_error_msg = Column(DateTime, nullable=True)  # not used
    save_text = Column(Boolean, nullable=True)
    max_count_per_interval = Column(Integer, nullable=False, default=1)
    min_post_interval_mins = Column(Integer, nullable=False, default=60 * 72)
    bot_mod = Column(String(21), nullable=True, default=None)  # most recent mod managing bot
    ban_ability = Column(Integer, nullable=False, default=-1)
    # -2 -> bans enabled but no perms -> blacklists instead of bans
    # -1 -> unknown (not yet checked)
    # 0 -> bans not enabled
    active_status = Column(SMALLINT, nullable=True)
    active_status_enum = Column(Enum(SubStatus))
    mm_convo_id = Column(String(10), nullable=True, default=None)
    is_nsfw = Column(Boolean, nullable=False, default=0)

    mod_list = Column(UnicodeText, nullable=True)  # added 7/4/22
    ignore_AutoModerator_removed = Column(Boolean, nullable=True, default=1)
    ignore_moderator_removed = Column(Boolean, nullable=True, default=1)
    exempt_self_posts = Column(Boolean, nullable=True)
    exempt_link_posts = Column(Boolean, nullable=True)
    exempt_oc = Column(Boolean, nullable=True)

    author_exempt_flair_keyword = Column(String(191), nullable=True, primary_key=False)
    author_not_exempt_flair_keyword = Column(String(191), nullable=True, primary_key=False)
    title_exempt_keyword = Column(String(191), nullable=True, primary_key=False)
    title_not_exempt_keyword = Column(String(191), nullable=True, primary_key=False)
    # self.last_updated = datetime.now() - timedelta(days=10)

    last_pulled = Column(DateTime, nullable=True)
    config_last_checked = Column(DateTime, nullable=True)

    subreddit_mods = []
    rate_limiting_enabled = False
    min_post_interval_hrs = 72
    min_post_interval_txt = ""
    min_post_interval = timedelta(hours=72)
    grace_period = timedelta(minutes=30)
    ban_duration_days = 0
    ban_threshold_count = 5
    notify_about_spammers = False

    action = None
    modmail = None
    message = None
    report_reason = None
    comment = None
    distinguish = True

    ignore_AutoModerator_removed = True

    exempt_moderator_posts = True

    modmail_posts_reply = True
    modmail_no_link_reply = False
    modmail_no_posts_reply = None
    modmail_no_posts_reply_internal = False
    modmail_notify_replied_internal = True
    modmail_auto_approve_messages_with_links = False
    modmail_all_reply = None
    modmail_removal_reason_helper = False
    modmail_receive_potential_predator_modmail = False
    approve = False
    blacklist_enabled = True
    lock_thread = True
    comment_stickied = False

    canned_responses = {}
    api_handle = None
    nsfw_instaban_subs = None

    nsfw_pct_instant_ban = False
    # nsfw_ban_duration_days = 0
    nsfw_pct_ban_duration_days = -1
    nsfw_pct_moderation = False
    nsfw_pct_threshold = 80

    # enforce_nsfw_checking = False

    def __init__(self, subreddit_name: str, sub_info=None):
        self.subreddit_name = subreddit_name.lower()
        self.save_text = False
        self.ignore_Automoderator_removed = True
        self.ignore_moderator_removed = True
        self.exempt_self_posts = False
        self.exempt_link_posts = False
        self.exempt_oc = False
        self.author_exempt_flair_keyword = None
        self.author_not_exempt_flair_keyword = None
        self.title_exempt_keyword = None
        self.title_not_exempt_keyword = None
        self.last_pulled = datetime.now(pytz.utc) - timedelta(hours=24)

        if not sub_info:
            self.active_status_enum = SubStatus.NO_CONFIG
        self.active_status_enum = sub_info.active_status_enum
        self.mod_list = sub_info.mod_list
        self.settings_yaml_txt = sub_info.settings_yaml_txt
        self.settings_revision_date = sub_info.settings_revision_date
        self.settings_yaml = sub_info.settings_yaml
        self.bot_mod = sub_info.bot_mod
        self.is_nsfw = sub_info.is_nsfw
        self.last_updated = datetime.now()
        self.reload_yaml_settings()

    def update_from_subinfo(self, sub_info):
        if not sub_info:
            self.active_status_enum = SubStatus.NO_CONFIG
            return False, f"no subinfo for {self.subreddit_name}"
        self.ignore_Automoderator_removed = True
        self.ignore_moderator_removed = True
        self.active_status_enum = sub_info.active_status_enum
        self.mod_list = sub_info.mod_list
        self.settings_yaml_txt = sub_info.settings_yaml_txt
        self.settings_revision_date = sub_info.settings_revision_date
        self.settings_yaml = sub_info.settings_yaml
        self.bot_mod = sub_info.bot_mod
        self.is_nsfw = sub_info.is_nsfw
        self.last_updated = datetime.now()
        if not self.last_pulled:
            self.last_pulled = datetime.now(pytz.utc) - timedelta(hours=24)

        return self.reload_yaml_settings()

    def reload_yaml_settings(self) -> (Boolean, String):
        if self.active_status_enum in (SubStatus.SUB_FORBIDDEN, SubStatus.SUB_GONE, SubStatus.CONFIG_ACCESS_ERROR):
            print(f"Sub access issue  {self.active_status_enum}")
            return False, f"Sub access issue  {self.active_status_enum}"
        return_text = "Updated Successfully!"
        if not self.settings_yaml_txt:
            self.active_status_enum = SubStatus.NO_CONFIG
            return False, "Nothing in yaml?"
        try:
            self.settings_yaml = yaml.safe_load(self.settings_yaml_txt)
        except (yaml.scanner.ScannerError, yaml.composer.ComposerError, yaml.parser.ParserError):
            self.active_status_enum = SubStatus.YAML_SYNTAX_ERROR
            return False, f"There is a syntax error in your config: " \
                          f"http://www.reddit.com/r/{self.subreddit_name}/wiki/{MAIN_BOT_NAME} ." \
                          f"Please validate your config using http://www.yamllint.com/. "
        if not self.settings_yaml:
            self.active_status_enum = SubStatus.YAML_SYNTAX_OK
            return False, "blank config?? settings yaml is None"

        if 'post_restriction' not in self.settings_yaml:

            self.active_status_enum = SubStatus.MHB_CONFIG_ERROR
            return False, f"Cannot load yaml config? {self.settings_yaml_txt} ||| {self.settings_yaml_txt}"

        self.ban_ability = -1

        if 'post_restriction' in self.settings_yaml:
            pr_settings = self.settings_yaml['post_restriction']
            self.rate_limiting_enabled = True
            possible_settings = {  # 'title_not_exempt_flair_keyword
                'max_count_per_interval': "int",
                'ignore_AutoModerator_removed': "bool",
                'ignore_moderator_removed': "bool",
                'ban_threshold_count': "int",
                'notify_about_spammers': "bool;int",
                'ban_duration_days': "int",
                'author_exempt_flair_keyword': "str;list",
                'author_not_exempt_flair_keyword': "str;list",
                'action': "str",
                'modmail': "str",
                'comment': "str",
                'message': "str",
                'report_reason': "str",
                'distinguish': "bool",
                'exempt_link_posts': "bool",
                'exempt_self_posts': "bool",
                'title_exempt_keyword': "str;list",
                'grace_period_mins': "int",
                'min_post_interval_hrs': "int",
                'min_post_interval_mins': "int",
                'approve': "bool",
                'lock_thread': "bool",
                'comment_stickied': "bool",
                'exempt_moderator_posts': "bool",
                'exempt_oc': "bool",
                'title_not_exempt_keyword': "str;list",
                'blacklist_enabled': "bool",

            }
            if not pr_settings:
                self.active_status_enum = SubStatus.MHB_CONFIG_ERROR
                return False, "No settings for post restriction"
            for pr_setting in pr_settings:
                if pr_setting in possible_settings:

                    pr_setting_value = pr_settings[pr_setting]
                    pr_setting_value = True if pr_setting_value == 'True' else pr_setting_value
                    pr_setting_value = False if pr_setting_value == 'False' else pr_setting_value

                    pr_setting_type = type(pr_setting_value).__name__
                    # if possible_settings[pr_setting] not in f"{type(pr_settings[pr_setting])}":  will not work for true for modmail stting to use default template
                    # if "min" in pr_setting or "hrs" in pr_setting\
                    #        and isinstance(pr_settings[pr_setting], str):

                    # print(f"{self.subreddit_name}: {pr_setting} {pr_setting_value} {pr_setting_type}, {possible_settings[pr_setting]}")
                    if pr_setting_type == "NoneType" or pr_setting_type in possible_settings[pr_setting].split(";"):
                        if isinstance(pr_setting_value, list):
                            # print([x for x in list])
                            pr_setting_value = "|".join(pr_setting_value)
                        setattr(self, pr_setting, pr_setting_value)

                    else:
                        return_text = f"{self.subreddit_name} invalid data type in your config: `{pr_setting}` which " \
                                      f"is written as `{pr_setting_value}` should be of type " \
                                      f"{possible_settings[pr_setting]} but is type {pr_setting_type}.  " \
                                      f"Make sure you use lowercase true and false"
                        print(return_text)
                        self.active_status_enum = SubStatus.MHB_CONFIG_ERROR
                        return False, return_text
                else:
                    return_text = "Did not recognize variable '{}' for {}".format(pr_setting, self.subreddit_name)
                    print(return_text)

            if 'min_post_interval_mins' in pr_settings:
                self.min_post_interval = timedelta(minutes=pr_settings['min_post_interval_mins'])
                self.min_post_interval_mins = pr_settings['min_post_interval_mins']
                self.min_post_interval_txt = f"{pr_settings['min_post_interval_mins']}m"
            if 'min_post_interval_hrs' in pr_settings:
                self.min_post_interval = timedelta(hours=pr_settings['min_post_interval_hrs'])
                self.min_post_interval_mins = pr_settings['min_post_interval_hrs'] * 60
                if self.min_post_interval_hrs < 24:
                    self.min_post_interval_txt = f"{self.min_post_interval_hrs}h"
                else:
                    self.min_post_interval_txt = f"{int(self.min_post_interval_hrs / 24)}d" \
                                                 f"{self.min_post_interval_hrs % 24}h".replace("d0h", "d")
            if 'grace_period_mins' in pr_settings and pr_settings['grace_period_mins'] is not None:
                self.grace_period = timedelta(minutes=pr_settings['grace_period_mins'])
                # self.grace_period_mins = pr_settings['grace_period_mins']
            if not self.ban_threshold_count:
                self.ban_threshold_count = 5

        if 'modmail' in self.settings_yaml:
            m_settings = self.settings_yaml['modmail']
            possible_settings = ('modmail_no_posts_reply', 'modmail_no_posts_reply_internal', 'modmail_posts_reply',
                                 'modmail_auto_approve_messages_with_links', 'modmail_all_reply',
                                 'modmail_notify_replied_internal', 'modmail_no_link_reply', 'canned_responses',
                                 'modmail_removal_reason_helper', 'modmail_receive_potential_predator_modmail')
            if m_settings:
                for m_setting in m_settings:
                    if m_setting in possible_settings:
                        setattr(self, m_setting, m_settings[m_setting])
                    else:
                        self.active_status_enum = SubStatus.MHB_CONFIG_ERROR
                        return_text = "Did not understand variable '{}'".format(m_setting)

        if 'nsfw_pct_moderation' in self.settings_yaml:
            n_settings = self.settings_yaml['nsfw_pct_moderation']
            self.nsfw_pct_moderation = True

            possible_settings = {
                'enforce_nsfw_checking': 'bool',
                'nsfw_pct_instant_ban': 'bool',
                'nsfw_pct_ban_duration_days': 'int',
                'nsfw_pct_threshold': 'int',
                'nsfw_instaban_subs': 'list',
                'nsfw_pct_set_user_flair': 'bool'
            }

            for n_setting in n_settings:
                if n_setting in possible_settings:
                    n_setting_value = n_settings[n_setting]
                    n_setting_value = True if n_setting_value == 'True' else n_setting_value
                    n_setting_value = False if n_setting_value == 'False' else n_setting_value

                    n_setting_type = type(n_setting_value).__name__
                    if n_setting_type == "NoneType" or n_setting_type in possible_settings[n_setting].split(";"):
                        setattr(self, n_setting, n_setting_value)

                    else:
                        return_text = f"{self.subreddit_name} invalid data type in yaml: `{n_setting}` which " \
                                      f"is written as `{n_setting_value}` should be of type " \
                                      f"{possible_settings[n_setting]} but is type {n_setting_type}.  " \
                                      f"Make sure you use lowercase true and false"
                        print(return_text)
                        self.active_status_enum = SubStatus.MHB_CONFIG_ERROR
                        return False, return_text
                else:
                    return_text = "Did not understand variable '{}' for {}".format(n_setting, self.subreddit_name)
                    return False, return_text

        self.min_post_interval = self.min_post_interval if self.min_post_interval else timedelta(hours=72)
        self.max_count_per_interval = self.max_count_per_interval if self.max_count_per_interval else 1
        self.active_status_enum = SubStatus.ACTIVE

        if self.ban_duration_days == 0:
            self.active_status_enum = SubStatus.MHB_CONFIG_ERROR
            return False, "ban_duration_days can no longer be zero. Use `ban_duration_days: ~` to disable or use " \
                          "`ban_duration_days: 999` for permanent bans. Make sure there is a space after the colon."

        return True, return_text

    def get_author_summary(self, wd, author_name: str) -> str:
        if author_name.startswith('u/'):
            author_name = author_name.replace("u/", "")

        recent_posts = s.query(SubmittedPost).filter(
            SubmittedPost.subreddit_name.ilike(self.subreddit_name),
            SubmittedPost.author == author_name,
            SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(days=182)).all()
        if not recent_posts:
            return "No posts found for {0} in {1}.".format(author_name, self.subreddit_name)
        diff = 0
        diff_str = "--"
        response_lines = [
            "For the last 4 months (since following this subreddit):\n\n|Time|Since Last|Author|Title|Status|\n"
            "|:-------|:-------|:------|:-----------|:------|\n"]
        for post in recent_posts:
            if diff != 0:
                diff_str = str(post.time_utc - diff)
            response_lines.append(
                "|{}|{}|u/{}|[{}]({})|{}|\n".format(post.time_utc, diff_str, post.author, post.title[0:15],
                                                    post.get_comments_url(),
                                                    wd.ri.get_posted_status(post).value))
            diff = post.time_utc
        response_lines.append(f"Current settings: {self.max_count_per_interval} post(s) "
                              f"per {self.min_post_interval_txt}")
        return "".join(response_lines)

    def get_sub_stats(self) -> str:
        total_reviewed = s.query(SubmittedPost) \
            .filter(SubmittedPost.subreddit_name.ilike(self.subreddit_name)) \
            .count()
        total_identified = s.query(SubmittedPost) \
            .filter(SubmittedPost.subreddit_name.ilike(self.subreddit_name)) \
            .filter(SubmittedPost.flagged_duplicate.is_(True)) \
            .count()

        authors = s.query(SubmittedPost, func.count(SubmittedPost.author).label('qty')) \
            .filter(SubmittedPost.subreddit_name.ilike(self.subreddit_name)) \
            .group_by(SubmittedPost.author).order_by(desc('qty')).limit(10).all().scalar()

        response_lines = ["Stats report for {0} \n\n".format(self.subreddit_name),
                          '|Author|Count|\n\n'
                          '|-----|----|']
        for post, count in authors:
            response_lines.append("|{}|{}|".format(post.author, count))

        return "total_reviewed: {}\n\n" \
               "total_identified: {}" \
               "\n\n{}".format(total_reviewed, total_identified, "\n\n".join(response_lines))

    def populate_tags(self, input_text, recent_post=None, prev_post=None, post_list=None):
        if not isinstance(input_text, str):
            print("error: {0} is not a string".format(input_text))
            return "error: `{0}` is not a string in your config".format(str(input_text))
        if post_list and not prev_post:
            prev_post = post_list[0]
        if post_list and "{summary table}" in input_text:
            response_lines = ["\n\n|ID|Time|Author|Title|Status|Counted?|\n"
                              "|:---|:-------|:------|:-----------|:------|:------|\n"]
            for post in post_list:
                response_lines.append(
                    f"|{post.id}"
                    f"|{post.time_utc}"
                    f"|[{post.author}](/u/{post.author})"
                    f"|[{post.title}]({post.get_comments_url()})"
                    f"|{post}"
                    f"|{CountedStatus(post.counted_status_enum)}"
                    f"|\n")
            final_response = "".join(response_lines)
            input_text = input_text.replace("{summary table}", final_response)

        if prev_post:
            input_text = input_text.replace("{prev.title}", prev_post.title)
            if prev_post.submission_text:
                input_text = input_text.replace("{prev.selftext}", prev_post.submission_text)
            input_text = input_text.replace("{prev.url}", prev_post.get_url())
            input_text = input_text.replace("{time}", prev_post.time_utc.strftime("%Y-%m-%d %H:%M:%S UTC"))
            input_text = input_text.replace("{timedelta}", humanize.naturaltime(
                datetime.now(pytz.utc) - prev_post.time_utc.replace(tzinfo=timezone.utc)))
        if recent_post:
            input_text = input_text.replace("{author}", recent_post.author)
            input_text = input_text.replace("{title}", recent_post.title)
            input_text = input_text.replace("{url}", recent_post.get_url())

        input_text = input_text.replace("{subreddit}", self.subreddit_name)
        input_text = input_text.replace("{maxcount}", "{0}".format(self.max_count_per_interval))
        input_text = input_text.replace("{interval}", "{0}m".format(self.min_post_interval_txt))
        return input_text

    def populate_tags2(self, input_text, recent_post=None, prev_post=None, post_list=None, wd=None):
        if not isinstance(input_text, str):
            print("error: {0} is not a string".format(input_text))
            return "error: `{0}` is not a string in your config".format(str(input_text))

        mydict = {"{subreddit}": self.subreddit_name, "{maxcount}": f"{self.max_count_per_interval}",
                  "{interval}": self.min_post_interval_txt}
        if recent_post:
            mydict.update({"{author}": recent_post.author, "{title}": recent_post.title,
                           "{url}": recent_post.get_url()})
        if post_list and not prev_post:
            prev_post = post_list[0]
        if post_list and "{summary table}" in input_text:
            response_lines = ["\n\n|ID|Time|Author|Title|Status|Counted?|\n"
                              "|:---|:-------|:------|:-----------|:------|:------|\n"]
            for post in post_list:
                posted_status = wd.ri.get_posted_status(post) if wd else None
                response_lines.append(
                    f"|{post.id}"
                    f"|{post.time_utc}"
                    f"|[{post.author}](/u/{post.author})"
                    f"|[{post.title}]({post.get_comments_url()})"
                    f"|{posted_status}"
                    # post.get_posted_status().value
                    f"|{CountedStatus(post.counted_status_enum)}"
                    f"|\n")
            final_response = "".join(response_lines)
            input_text = input_text.replace("{summary table}", final_response)

        if prev_post:
            if prev_post.submission_text:
                mydict["{prev.selftext}"] = prev_post.submission_text
            mydict.update({"{prev.title}": prev_post.title, "{prev.url}": prev_post.get_url(),
                           "{time}": prev_post.time_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                           "{timedelta}": humanize.naturaltime(datetime.now(pytz.utc)
                                                               - prev_post.time_utc.replace(tzinfo=timezone.utc)),
                           })

        input_text = re.sub(r'{(.+?)}', lambda m: mydict.get(m.group(), m.group()), input_text)
        input_text = input_text.replace("\\n", "\n\n")
        return input_text
