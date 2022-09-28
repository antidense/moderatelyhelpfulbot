
from praw import exceptions
from praw.models import Submission

import praw
import yaml
import prawcore
from logger import logger
from enums import SubStatus, PostedStatus, CountedStatus


from models.reddit_models import SubmittedPost, TrackedSubreddit, TrackedAuthor

from settings import MAIN_BOT_NAME
from typing import List
from datetime import datetime
import pytz
# Set up PRAW

BOT_NAME = None


class RedditInterface:
    bot_sub = None
    reddit_client = None
    bot_name = None

    def __init__(self):
        self.reddit_client = praw.Reddit(
                                    )
        BOT_NAME = self.reddit_client.user.me().name
        self.bot_name = self.reddit_client.user.me().name

    '''SUBMISSION STUFF'''
    def get_submission_api_handle(self, submission: SubmittedPost) -> praw.models.Submission:
        if not submission.api_handle:
            submission.api_handle = self.reddit_client.submission(id=submission.id)
            return submission.api_handle
        else:
            return submission.api_handle

    def update_posted_status(self, submission: SubmittedPost):
        post_api_handle = self.get_submission_api_handle(submission)
        try:
            submission.self_deleted = False if submission.api_handle.author else True
        except prawcore.exceptions.Forbidden:
            submission.posted_status = PostedStatus.UNAVAILABLE.value
        submission.banned_by = submission.api_handle.banned_by
        if not submission.banned_by and not submission.self_deleted:
            submission.posted_status = PostedStatus.UP.value
        elif submission.banned_by:
            if submission.banned_by is True:
                submission.posted_status =  PostedStatus.SPAM_FLT.value
            elif submission.banned_by == "AutoModerator":
                submission.posted_status =   PostedStatus.AUTOMOD_RM.value
            elif submission.banned_by in (self.bot_name, MAIN_BOT_NAME):
                submission.posted_status = PostedStatus.MHB_RM.value
            else:
                submission.posted_status =   PostedStatus.MOD_RM.value
        elif submission.self_deleted:
            submission.posted_status =   PostedStatus.SELF_DEL.value
        else:
            print(f"unknown status: {submission.banned_by}")
            submission.posted_status =   PostedStatus.UNKNOWN.value
        submission.post_flair = post_api_handle.link_flair_text
        submission.author_flair = post_api_handle.author_flair_text
        if submission.counted_status == 1:
            submission.counted_status = CountedStatus.NEEDS_UPDATE.value
        submission.last_checked = datetime.now(pytz.utc)



    def get_posted_status(self, submission: SubmittedPost, get_removed_info=False) -> PostedStatus:
        print(f'getting posted status...  current status:{submission.posted_status}')
        # _ = submission.get_api_handle()  what was this for again?
        post_api_handle = self.get_submission_api_handle(submission)  # updates the api handle
        try:
            submission.self_deleted = False if submission.api_handle.author else True
        except prawcore.exceptions.Forbidden:
            return PostedStatus.UNKNOWN
        submission.banned_by = submission.api_handle.banned_by
        if not submission.banned_by and not submission.self_deleted:
            return PostedStatus.UP
        elif submission.banned_by:
            if submission.banned_by is True:
                return PostedStatus.SPAM_FLT
            if not submission.bot_comment_id and get_removed_info:  # make sure to commit to db
                top_level_comments = list(submission.api_handle.comments)
                for c in top_level_comments:
                    if hasattr(c, 'author') and c.author and c.author.name == submission.banned_by:
                        submission.bot_comment_id = c.id
                        break
            if submission.banned_by == "AutoModerator":
                return PostedStatus.AUTOMOD_RM
            elif submission.banned_by == "Flair_Helper":
                return PostedStatus.FH_RM
            elif submission.banned_by in (self.bot_name, MAIN_BOT_NAME):
                return PostedStatus.MHB_RM
            elif "bot" in submission.banned_by.lower():
                return PostedStatus.BOT_RM
            else:
                return PostedStatus.MOD_RM
        elif submission.self_deleted:
            return PostedStatus.SELF_DEL
        else:
            print(f"unknown status: {submission.banned_by}")
            return PostedStatus.UNKNOWN

    def mod_remove(self, submission: SubmittedPost) -> bool:
        _ = self.get_submission_api_handle(submission)  # updates the api handle
        try:
            submission.api_handle.mod.remove()
            return True
        except praw.exceptions.APIException:
            logger.warning(f'something went wrong removing post: http://redd.it/{submission.id}')
            return False
        except (prawcore.exceptions.Forbidden, prawcore.exceptions.ServerError):
            logger.warning(f'I was not allowed to remove the post: http://redd.it/{submission.id}')
            return False

    def reply(self, submission, response, distinguish=True, approve=False, lock_thread=True):
        _ = self.get_submission_api_handle(submission)  # updates the api handle
        try:
            # first try to lock thread - useless to make a comment unless it's possible
            if lock_thread:
                submission.api_handle.mod.lock()
            comment = submission.api_handle.reply(body=response)
            if comment and distinguish:
                comment.mod.distinguish()
            if comment and approve:
                comment.mod.approve()
            return comment
        except praw.exceptions.APIException:
            logger.warning(f'Something went wrong with replying to this post: http://redd.it/{submission.id}')
            return False
        except (prawcore.exceptions.Forbidden, prawcore.exceptions.ServerError):
            logger.warning(f'Something with replying to this post:: http://redd.it/{submission.id}')
            return False
        except (prawcore.exceptions.BadRequest):
            logger.warning(f'Something with replying to this post:: http://redd.it/{submission.id}')
            return False

    """REDDITOR STUFF"""
    def get_author_api_handle(self, author: TrackedAuthor) -> praw.models.Redditor:
        if not author.api_handle:
            author.api_handle = self.reddit_client.redditor(author.author_name)
            return author.api_handle
        else:
            return author.api_handle

    """SUBREDDIT STUFF"""
    def get_subreddit_str_api_handle(self, subreddit_name) -> praw.models.Subreddit:
            return self.reddit_client.subreddit(subreddit_name)

    def get_subreddit_api_handle(self, subreddit: TrackedSubreddit) -> praw.models.Subreddit:
        assert(isinstance(subreddit,TrackedSubreddit))
        if not subreddit.api_handle:
            subreddit.api_handle = self.reddit_client.subreddit(subreddit.subreddit_name)
            return subreddit.api_handle
        else:
            return subreddit.api_handle

    def get_subreddit_info(self, subreddit_name=None):
        si = SubredditInfo(ri=self,  subreddit_name=subreddit_name)
        return si


    def get_modmail_thread_id(self, subreddit_name=None):
        for convo in self.reddit_client.subreddit(subreddit_name).modmail.conversations(state="mod", sort='unread', limit=30):

            initiating_author_name = convo.authors[0].name  # praw query
            #subreddit_name = convo.owner.display_name  # praw query
            if initiating_author_name == self.bot_name and convo.id !="11ejht":
                return convo.id

    def send_modmail(self, subreddit=None, subreddit_name=None, subject=None, body = "Unspecified text",
                     thread_id=None, use_same_thread=False):
        conversation = None
        if subject is None:
            subject = f"[Notification] Message from {self.bot_name}"
        # assert isinstance(subreddit, TrackedSubreddit)
        if subreddit_name in (self.bot_name, MAIN_BOT_NAME):
            subreddit = self.bot_sub
            thread_id = subreddit.mm_convo_id

        if subreddit and not thread_id and use_same_thread:
            thread_id = subreddit.mm_convo_id


        if not subreddit_name and subreddit:
            subreddit_name = subreddit.subreddit_name
        if thread_id:
            conversation = self.reddit_client.subreddit(subreddit_name).modmail(thread_id).reply(body, internal=True)
        else:
            try:
                conversation = self.reddit_client.subreddit(subreddit_name).message(subject=subject, message=body)
                if subreddit:
                    subreddit.mm_convo_id = conversation.id  # won't get saved?
            except (praw.exceptions.APIException, prawcore.exceptions.Forbidden, AttributeError):
                logger.warning('something went wrong in sending modmail')

        return conversation



    def send_message(self, redditor, subject, message):
        if isinstance(redditor, str):
            redditor = self.reddit_client.redditor(redditor)
        redditor.message(message=message, subject=subject)

    def get_removed_explanation(self, submittedpost):
        if not submittedpost.bot_comment_id:
            return None
        comment = self.reddit_client.comment(submittedpost.bot_comment_id)
        if comment and comment.body:
            return comment.body
        else:
            return None
    def get_mod_list(self, subreddit_name=None, subreddit=None) -> List[str]:
        if subreddit and not subreddit_name:
            subreddit_name = subreddit.subreddit_name
        try:
            return list(moderator.name for moderator in self.reddit_client.subreddit(subreddit_name).moderator())
        except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden):
            return None



class SubmissionInfo:
    def __init__(self, submission):
        subm_api_handle = None
        # static
        self.id = submission.id
        self.title = submission.title[0:190]
        self.submission_text = submission.selftext[0:190]
        self.time_utc = datetime.utcfromtimestamp(submission.created_utc)
        self.subreddit_name = str(submission.subreddit).lower()
        self.is_self = submission.is_self
        # self.is_nsfw = submission.over18
        self.is_oc = submission.is_original_content

        # may change
        self.author = str(submission.author)  # don't change once deleted
        self.banned_by = submission.banned_by
        self.post_flair = submission.link_flair_text
        self.author_flair = submission.author_flair_text


    def update(self, submission):
        # self.author = str(submission.author)
        self.banned_by = submission.banned_by
        self.post_flair = submission.link_flair_text
        self.author_flair = submission.author_flair_text

class SubredditInfo:
    subreddit_api_handle = None
    active_status = SubStatus.UNKNOWN.value
    subreddit_name = None
    mod_list = None
    settings_yaml_txt = None
    settings_revision_date = None
    settings_yaml = None
    bot_mod = None
    is_nsfw = False

    def __init__(self, ri, subreddit_name):
        if subreddit_name.startswith("/r/"):
            subreddit_name = subreddit_name.replace('/r/', '')
        self.subreddit_name: str = subreddit_name.lower()
        self.subreddit_api_handle = ri.reddit_client.subreddit(subreddit_name)
        if not self.subreddit_api_handle:  # Subreddit doesn't exist
            active_status = SubStatus.SUB_GONE.value
            return
        try:
            self.mod_list = ",".join(list(moderator.name for moderator in self.subreddit_api_handle.moderator()))
        except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden, prawcore.exceptions.Redirect):
            return None
        active_status, response = self.check_sub_access(ri)

        print(f"ri/csa: sub {subreddit_name} has this issue: {response}")
        # print(f"yaml: {self.settings_yaml_txt}")
        if active_status.value > 0:
            self.is_nsfw = self.subreddit_api_handle.over18


    def check_sub_access(self, ri, ignore_no_mod_access=False) -> (SubStatus, str):
        mod_list = ri.get_mod_list(subreddit_name=self.subreddit_name)
        if not mod_list:
            self.active_status = SubStatus.SUB_FORBIDDEN.value
            return SubStatus.SUB_FORBIDDEN, f"Subreddit is banned."
        if ignore_no_mod_access and ri.bot_name not in mod_list:
            self.active_status = SubStatus.NO_MOD_PRIV.value

            return SubStatus.NO_MOD_PRIV, f"The bot does not have moderator privileges to /r/{self.subreddit_name}."
        try:
            logger.debug(f'si/csa accessing wiki config {self.subreddit_name}, {MAIN_BOT_NAME}')

            # logger.debug(f'si/csa wiki_page {wiki_page.content_md}')
            wiki_page = None
            wiki_pages = [x.name for x in self.subreddit_api_handle.wiki]
            for wiki_page_name in wiki_pages:
                print(f"wiki page name: {wiki_page_name}")
                if wiki_page_name.lower() == MAIN_BOT_NAME.lower() or wiki_page_name.lower() == ri.bot_name.lower():
                    wiki_page=self.subreddit_api_handle.wiki[wiki_page_name]
                    break
            if wiki_page:
                self.settings_yaml_txt = wiki_page.content_md
                #logger.debug(f'si/csa wiki_page {wiki_page.content_md}')
                self.settings_revision_date = wiki_page.revision_date
                if wiki_page.revision_by and wiki_page.revision_by.name != ri.bot_name:
                    self.bot_mod = wiki_page.revision_by.name
                self.settings_yaml = yaml.safe_load(self.settings_yaml_txt)
            else:
                wiki_page = self.subreddit_api_handle.wiki[ri.bot_name]
                if not wiki_page:
                    return SubStatus.NO_CONFIG, f"I only found an empty config for /r/{self.subreddit_name}."
        except prawcore.exceptions.NotFound:
            return SubStatus.NO_CONFIG, f"I did not find a config for /r/{self.subreddit_name} Please create one at" \
                                        f"http://www.reddit.com/r/{self.subreddit_name}/wiki/{ri.bot_name} ."
        except prawcore.exceptions.Forbidden:
            return SubStatus.CONFIG_ACCESS_ERROR, f"I do not have any access to /r/{self.subreddit_name}."
        except prawcore.exceptions.Redirect:
            return SubStatus.SUB_GONE, f"Reddit reports that there is no subreddit by the name of {self.subreddit_name}."
        except (yaml.scanner.ScannerError, yaml.composer.ComposerError, yaml.parser.ParserError):
            return SubStatus.YAML_SYNTAX_ERROR, f"There is a syntax error in your config: " \
                                                f"http://www.reddit.com/r/{self.subreddit_name}/wiki/{ri.bot_name} ." \
                                                f"Please validate your config using http://www.yamllint.com/. "
        return SubStatus.YAML_SYNTAX_OK, "Syntax is valid"


