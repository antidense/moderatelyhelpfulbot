import re
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import humanize
import praw
import prawcore
import pytz
import yaml
from core import BOT_NAME
from database import Base, get_session
from enums import CountedStatus, SubStatus
from logger import logger
from models import SubmittedPost
from reddit import REDDIT_CLIENT
from sqlalchemy import (SMALLINT, Boolean, Column, DateTime, Integer, String,
                        UnicodeText, desc, func, true)

s = get_session()


class TrackedSubreddit(Base):
    __tablename__ = "TrackedSubs"
    subreddit_name = Column(String(21), nullable=False, primary_key=True)
    checking_mail_enabled = Column(Boolean, nullable=True)  # don't need this?
    settings_yaml_txt = Column(UnicodeText, nullable=True)
    settings_yaml = None
    last_updated = Column(DateTime, nullable=True)
    last_error_msg = Column(DateTime, nullable=True)  # not used
    save_text = Column(Boolean, nullable=True)
    max_count_per_interval = Column(Integer, nullable=False, default=1)
    min_post_interval_mins = Column(Integer, nullable=False, default=60 * 72)
    bot_mod = Column(
        String(21), nullable=True, default=None
    )  # most recent mod managing bot
    ban_ability = Column(Integer, nullable=False, default=-1)
    # -2 -> bans enabled but no perms -> blacklists instead of bans
    # -1 -> unknown (not yet checked)
    # 0 -> bans not enabled
    active_status = Column(SMALLINT, nullable=True)
    mm_convo_id = Column(String(10), nullable=True, default=None)
    is_nsfw = Column(Boolean, nullable=False, default=0)

    subreddit_mods = []
    rate_limiting_enabled = False
    min_post_interval_hrs = 72
    min_post_interval_txt = ""
    min_post_interval = timedelta(hours=72)
    grace_period = timedelta(minutes=30)
    ban_duration_days = 0
    ignore_AutoModerator_removed = True
    ignore_moderator_removed = True
    ban_threshold_count = 5
    notify_about_spammers = False
    author_exempt_flair_keyword = None
    author_not_exempt_flair_keyword = None
    title_exempt_keyword = None
    action = None
    modmail = None
    message = None
    report_reason = None
    comment = None
    distinguish = True
    exempt_self_posts = False
    exempt_link_posts = False
    exempt_moderator_posts = True
    exempt_oc = False
    modmail_posts_reply = True
    modmail_no_link_reply = False
    modmail_no_posts_reply = None
    modmail_no_posts_reply_internal = False
    modmail_notify_replied_internal = True
    modmail_auto_approve_messages_with_links = False
    modmail_all_reply = None
    modmail_removal_reason_helper = False
    approve = False
    blacklist_enabled = True
    lock_thread = True
    comment_stickied = False
    title_not_exempt_keyword = None
    canned_responses = {}
    api_handle = None
    instaban_subs = None

    def __init__(self, subreddit_name: str):
        self.subreddit_name = subreddit_name.lower()
        self.save_text = False
        self.last_updated = datetime(2019, 1, 1, 0, 0)
        self.update_from_yaml(force_update=True)
        self.settings_revision_date = None
        self.api_handle = REDDIT_CLIENT.subreddit(self.subreddit_name)
        self.active_status = SubStatus.UNKNOWN.value

    def get_mods_list(self, subreddit_handle=None) -> List[str]:
        self.api_handle = (
            REDDIT_CLIENT.subreddit(self.subreddit_name)
            if not self.api_handle
            else self.api_handle
        )
        try:
            return list(moderator.name for moderator in self.api_handle.moderator())
        except prawcore.exceptions.NotFound:
            return []

    def check_access(self) -> SubStatus:
        api_handle = (
            REDDIT_CLIENT.subreddit(self.subreddit_name)
            if not self.api_handle
            else self.api_handle
        )
        if not api_handle:  # Subreddit doesn't exist
            return SubStatus.SUB_GONE
        self.api_handle = api_handle  # Else keep the reference to the subreddit
        if BOT_NAME not in self.get_mods_list():
            self.active_status = SubStatus.NOT_MOD.value
            return SubStatus.NOT_MOD
        try:
            logger.warning(f"accessing wiki config {self.subreddit_name}")
            wiki_page = self.api_handle.wiki[BOT_NAME]
            _ = wiki_page.revision_date
            settings_yaml = yaml.safe_load(wiki_page.content_md)
        except prawcore.exceptions.NotFound:
            return SubStatus.NO_CONFIG
        except prawcore.exceptions.Forbidden:
            return SubStatus.CONFIG_ACCESS_ERROR
        except prawcore.exceptions.Redirect:
            print(f"Redirect for {self.subreddit_name}")
            return SubStatus.SUB_GONE
        except (
            yaml.scanner.ScannerError,
            yaml.composer.ComposerError,
            yaml.parser.ParserError,
        ):
            return SubStatus.CONFIG_ERROR
        return SubStatus.YAML_SYNTAX_OKAY

    def update_access(self):
        active_status = self.check_access()

        # if active_status == SubStatus.YAML_SYNTAX_OKAY and self.active_status == 10:
        #    return
        self.active_status = active_status.value
        s.add(self)
        s.commit()
        return active_status

    def update_from_yaml(self, force_update: bool = False) -> Tuple[Boolean, String]:
        return_text = "Updated Successfully!"
        self.api_handle = (
            REDDIT_CLIENT.subreddit(self.subreddit_name)
            if not self.api_handle
            else self.api_handle
        )
        try:
            self.is_nsfw = self.api_handle.over18
        except prawcore.exceptions.NotFound:
            print(f"do not know if {self.subreddit_name} is over 18...")

            self.is_nsfw = None
        self.ban_ability = -1
        # self.active_status = 20
        self.subreddit_mods = self.get_mods_list(subreddit_handle=self.api_handle)

        if force_update or self.settings_yaml_txt is None:
            try:
                logger.warning(f"accessing wiki config {self.subreddit_name}")
                wiki_page = REDDIT_CLIENT.subreddit(self.subreddit_name).wiki[BOT_NAME]
                if wiki_page:
                    self.settings_yaml_txt = wiki_page.content_md
                    self.settings_revision_date = wiki_page.revision_date
                    if wiki_page.revision_by:
                        self.bot_mod = wiki_page.revision_by.name
                    else:
                        self.active_status = SubStatus.NO_CONFIG.value
            except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden) as e:
                logger.warning(f"no config accessible for {self.subreddit_name}")
                self.rate_limiting_enabled = False
                self.active_status = SubStatus.CONFIG_ACCESS_ERROR.value

                return False, str(e)

        if self.settings_yaml_txt is None:
            return (
                False,
                "Is the wiki updated? I could not find any settings in the wiki",
            )
        try:
            self.settings_yaml = yaml.safe_load(self.settings_yaml_txt)
        except (
            yaml.scanner.ScannerError,
            yaml.composer.ComposerError,
            yaml.parser.ParserError,
        ) as e:
            return False, str(e)

        if self.settings_yaml is None:
            return False, "I couldn't get settings from the wiki for some reason :/"

        if "save_text" in self.settings_yaml:
            self.save_text = self.settings_yaml["save_text"]
            # print(self.save_text)

        if "post_restriction" in self.settings_yaml:
            pr_settings = self.settings_yaml["post_restriction"]
            self.rate_limiting_enabled = True
            possible_settings = {
                "max_count_per_interval": "int",
                "ignore_AutoModerator_removed": "bool",
                "ignore_moderator_removed": "bool",
                "ban_threshold_count": "int",
                "notify_about_spammers": "bool;int",
                "ban_duration_days": "int",
                "author_exempt_flair_keyword": "str;list",
                "author_not_exempt_flair_keyword": "str;list",
                "action": "str",
                "modmail": "str",
                "comment": "str",
                "message": "str",
                "report_reason": "str",
                "distinguish": "bool",
                "exempt_link_posts": "bool",
                "exempt_self_posts": "bool",
                "title_exempt_keyword": "str;list",
                "grace_period_mins": "int",
                "min_post_interval_hrs": "int",
                "min_post_interval_mins": "int",
                "approve": "bool",
                "lock_thread": "bool",
                "comment_stickied": "bool",
                "exempt_moderator_posts": "bool",
                "exempt_oc": "bool",
                "title_not_exempt_keyword": "str;list",
                "blacklist_enabled": "bool",
            }
            if not pr_settings:
                return False, "Bad config"
            for pr_setting in pr_settings:
                if pr_setting in possible_settings:
                    pr_setting_value = pr_settings[pr_setting]
                    pr_setting_value = (
                        True if pr_setting_value == "True" else pr_setting_value
                    )
                    pr_setting_value = (
                        False if pr_setting_value == "False" else pr_setting_value
                    )

                    pr_setting_type = type(pr_setting_value).__name__
                    # if possible_settings[pr_setting] not in f"{type(pr_settings[pr_setting])}":  will not work for true for modmail stting to use default template
                    # if "min" in pr_setting or "hrs" in pr_setting\
                    #        and isinstance(pr_settings[pr_setting], str):

                    # print(f"{self.subreddit_name}: {pr_setting} {pr_setting_value} {pr_setting_type}, {possible_settings[pr_setting]}")
                    if (
                        pr_setting_type == "NoneType"
                        or pr_setting_type in possible_settings[pr_setting].split(";")
                    ):
                        setattr(self, pr_setting, pr_setting_value)

                    else:
                        return_text = (
                            f"{self.subreddit_name} invalid data type in yaml: `{pr_setting}` which "
                            f"is written as `{pr_setting_value}` should be of type "
                            f"{possible_settings[pr_setting]} but is type {pr_setting_type}.  "
                            f"Make sure you use lowercase true and false"
                        )
                        print(return_text)
                        return False, return_text
                else:
                    return_text = "Did not understand variable '{}' for {}".format(
                        pr_setting, self.subreddit_name
                    )
                    print(return_text)

            if "min_post_interval_mins" in pr_settings:
                self.min_post_interval = timedelta(
                    minutes=pr_settings["min_post_interval_mins"]
                )
                self.min_post_interval_txt = f"{pr_settings['min_post_interval_mins']}m"
            if "min_post_interval_hrs" in pr_settings:
                self.min_post_interval = timedelta(
                    hours=pr_settings["min_post_interval_hrs"]
                )
                if self.min_post_interval_hrs < 24:
                    self.min_post_interval_txt = f"{self.min_post_interval_hrs}h"
                else:
                    self.min_post_interval_txt = (
                        f"{int(self.min_post_interval_hrs / 24)}d"
                        f"{self.min_post_interval_hrs % 24}h".replace("d0h", "d")
                    )
            if (
                "grace_period_mins" in pr_settings
                and pr_settings["grace_period_mins"] is not None
            ):
                self.grace_period = timedelta(minutes=pr_settings["grace_period_mins"])
                # self.grace_period_mins = pr_settings['grace_period_mins']
            if not self.ban_threshold_count:
                self.ban_threshold_count = 5

        if "modmail" in self.settings_yaml:
            m_settings = self.settings_yaml["modmail"]
            possible_settings = (
                "modmail_no_posts_reply",
                "modmail_no_posts_reply_internal",
                "modmail_posts_reply",
                "modmail_auto_approve_messages_with_links",
                "modmail_all_reply",
                "modmail_notify_replied_internal",
                "modmail_no_link_reply",
                "canned_responses",
                "modmail_removal_reason_helper",
            )
            if m_settings:
                for m_setting in m_settings:
                    if m_setting in possible_settings:
                        setattr(self, m_setting, m_settings[m_setting])
                    else:
                        return_text = "Did not understand variable '{}'".format(
                            m_setting
                        )
        if "history_checking" in self.settings_yaml:
            h_settings = self.settings_yaml["history_checking"]
            possible_settings = ("instaban_subs",)
            if h_settings:
                for h_setting in h_settings:
                    if h_setting in possible_settings:
                        setattr(self, h_setting, h_settings[h_setting])
                    else:
                        return_text = "Did not understand variable '{}'".format(
                            h_setting
                        )

        self.min_post_interval = (
            self.min_post_interval if self.min_post_interval else timedelta(hours=72)
        )
        self.max_count_per_interval = (
            self.max_count_per_interval if self.max_count_per_interval else 1
        )
        mods_list = self.get_mods_list()
        if BOT_NAME not in mods_list:
            return (
                False,
                "I do not currently have mod privileges yet. If you just added me, please wait for approval",
            )
        self.active_status = SubStatus.ACTIVE.value
        self.last_updated = datetime.now()
        if self.ban_duration_days == 0:
            return (
                False,
                "ban_duration_days can no longer be zero. Use `ban_duration_days: ~` to disable or use "
                "`ban_duration_days: 999` for permanent bans. Make sure there is a space after the colon.",
            )

        return True, return_text

    @staticmethod
    def get_subreddit_by_name(subreddit_name: str, create_if_not_exist=True):
        if subreddit_name.startswith("/r/"):
            subreddit_name = subreddit_name.replace("/r/", "")
        subreddit_name: str = subreddit_name.lower()
        tr_sub: TrackedSubreddit = s.query(TrackedSubreddit).get(subreddit_name)
        if not tr_sub:  # does not exist in database
            if not create_if_not_exist:
                return None
            try:
                tr_sub = TrackedSubreddit(subreddit_name)
                return tr_sub
            except prawcore.PrawcoreException:
                return None
        else:
            successful, message = tr_sub.update_from_yaml(
                force_update=False
            )  # load variables from stored yaml
        return tr_sub

    def get_author_summary(self, author_name: str) -> str:
        if author_name.startswith("u/"):
            author_name = author_name.replace("u/", "")

        recent_posts = (
            s.query(SubmittedPost)
            .filter(
                SubmittedPost.subreddit_name.ilike(self.subreddit_name),
                SubmittedPost.author == author_name,
                SubmittedPost.time_utc > datetime.now(pytz.utc) - timedelta(days=182),
            )
            .all()
        )
        if not recent_posts:
            return "No posts found for {0} in {1}.".format(
                author_name, self.subreddit_name
            )
        diff = 0
        diff_str = "--"
        response_lines = [
            "For the last 4 months (since following this subreddit):\n\n|Time|Since Last|Author|Title|Status|\n"
            "|:-------|:-------|:------|:-----------|:------|\n"
        ]
        for post in recent_posts:
            if diff != 0:
                diff_str = str(post.time_utc - diff)
            response_lines.append(
                "|{}|{}|u/{}|[{}]({})|{}|\n".format(
                    post.time_utc,
                    diff_str,
                    post.author,
                    post.title[0:15],
                    post.get_comments_url(),
                    post.get_posted_status().value,
                )
            )
            diff = post.time_utc
        response_lines.append(
            f"Current settings: {self.max_count_per_interval} post(s) "
            f"per {self.min_post_interval_txt}"
        )
        return "".join(response_lines)

    def get_sub_stats(self) -> str:
        total_reviewed = (
            s.query(SubmittedPost)
            .filter(SubmittedPost.subreddit_name.ilike(self.subreddit_name))
            .count()
        )
        total_identified = (
            s.query(SubmittedPost)
            .filter(SubmittedPost.subreddit_name.ilike(self.subreddit_name))
            .filter(SubmittedPost.flagged_duplicate.is_(True))
            .count()
        )

        authors = (
            s.query(SubmittedPost, func.count(SubmittedPost.author).label("qty"))
            .filter(SubmittedPost.subreddit_name.ilike(self.subreddit_name))
            .group_by(SubmittedPost.author)
            .order_by(desc("qty"))
            .limit(10)
            .all()
            .scalar()
        )

        response_lines = [
            "Stats report for {0} \n\n".format(self.subreddit_name),
            "|Author|Count|\n\n" "|-----|----|",
        ]
        for post, count in authors:
            response_lines.append("|{}|{}|".format(post.author, count))

        return (
            "total_reviewed: {}\n\n"
            "total_identified: {}"
            "\n\n{}".format(
                total_reviewed, total_identified, "\n\n".join(response_lines)
            )
        )

    def send_modmail(
        self,
        subject=f"[Notification] Message from {BOT_NAME}",
        body="Unspecfied text",
        thread_id=None,
    ):
        if thread_id:
            REDDIT_CLIENT.subreddit(self.subreddit_name).modmail(thread_id).reply(
                body, internal=true
            )
        else:
            try:
                REDDIT_CLIENT.subreddit(self.subreddit_name).message(subject, body)
            except (
                praw.exceptions.APIException,
                prawcore.exceptions.Forbidden,
                AttributeError,
            ):
                logger.warning("something went wrong in sending modmail")

    def populate_tags(
        self, input_text, recent_post=None, prev_post=None, post_list=None
    ):
        if not isinstance(input_text, str):
            print("error: {0} is not a string".format(input_text))
            return "error: `{0}` is not a string in your config".format(str(input_text))
        if post_list and not prev_post:
            prev_post = post_list[0]
        if post_list and "{summary table}" in input_text:
            response_lines = [
                "\n\n|ID|Time|Author|Title|Status|Counted?|\n"
                "|:---|:-------|:------|:-----------|:------|:------|\n"
            ]
            for post in post_list:
                response_lines.append(
                    f"|{post.id}"
                    f"|{post.time_utc}"
                    f"|[{post.author}](/u/{post.author})"
                    f"|[{post.title}]({post.get_comments_url()})"
                    f"|{post.get_posted_status().value}"
                    f"|{CountedStatus(post.counted_status)}"
                    f"|\n"
                )
            final_response = "".join(response_lines)
            input_text = input_text.replace("{summary table}", final_response)

        if prev_post:
            input_text = input_text.replace("{prev.title}", prev_post.title)
            if prev_post.submission_text:
                input_text = input_text.replace(
                    "{prev.selftext}", prev_post.submission_text
                )
            input_text = input_text.replace("{prev.url}", prev_post.get_url())
            input_text = input_text.replace(
                "{time}", prev_post.time_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
            )
            input_text = input_text.replace(
                "{timedelta}",
                humanize.naturaltime(
                    datetime.now(pytz.utc)
                    - prev_post.time_utc.replace(tzinfo=timezone.utc)
                ),
            )
        if recent_post:
            input_text = input_text.replace("{author}", recent_post.author)
            input_text = input_text.replace("{title}", recent_post.title)
            input_text = input_text.replace("{url}", recent_post.get_url())

        input_text = input_text.replace("{subreddit}", self.subreddit_name)
        input_text = input_text.replace(
            "{maxcount}", "{0}".format(self.max_count_per_interval)
        )
        input_text = input_text.replace(
            "{interval}", "{0}m".format(self.min_post_interval_txt)
        )
        return input_text

    def populate_tags2(
        self, input_text, recent_post=None, prev_post=None, post_list=None
    ):
        if not isinstance(input_text, str):
            print("error: {0} is not a string".format(input_text))
            return "error: `{0}` is not a string in your config".format(str(input_text))

        mydict = {
            "{subreddit}": self.subreddit_name,
            "{maxcount}": f"{self.max_count_per_interval}",
            "{interval}": self.min_post_interval_txt,
        }
        if recent_post:
            mydict.update(
                {
                    "{author}": recent_post.author,
                    "{title}": recent_post.title,
                    "{url}": recent_post.get_url(),
                }
            )
        if post_list and not prev_post:
            prev_post = post_list[0]
        if post_list and "{summary table}" in input_text:
            response_lines = [
                "\n\n|ID|Time|Author|Title|Status|Counted?|\n"
                "|:---|:-------|:------|:-----------|:------|:------|\n"
            ]
            for post in post_list:
                response_lines.append(
                    f"|{post.id}"
                    f"|{post.time_utc}"
                    f"|[{post.author}](/u/{post.author})"
                    f"|[{post.title}]({post.get_comments_url()})"
                    f"|{post.get_posted_status().value}"
                    f"|{CountedStatus(post.counted_status)}"
                    f"|\n"
                )
            final_response = "".join(response_lines)
            input_text = input_text.replace("{summary table}", final_response)

        if prev_post:
            if prev_post.submission_text:
                mydict["{prev.selftext}"] = prev_post.submission_text
            mydict.update(
                {
                    "{prev.title}": prev_post.title,
                    "{prev.url}": prev_post.get_url(),
                    "{time}": prev_post.time_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "{timedelta}": humanize.naturaltime(
                        datetime.now(pytz.utc)
                        - prev_post.time_utc.replace(tzinfo=timezone.utc)
                    ),
                }
            )

        input_text = re.sub(
            r"{(.+?)}", lambda m: mydict.get(m.group(), m.group()), input_text
        )
        return input_text

    def get_api_handle(self):
        if not self.api_handle:
            self.api_handle = REDDIT_CLIENT.subreddit(self.subreddit_name)
            return self.api_handle
        else:
            return self.api_handle
