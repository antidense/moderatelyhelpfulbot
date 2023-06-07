from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

import prawcore.exceptions
from praw import exceptions

from logger import logger
from models.reddit_models import ActionedComments, SubAuthor, SubmittedPost, TrackedSubreddit, LoggedAction
from settings import MAIN_BOT_NAME, ACCEPTING_NEW_SUBS, BOT_OWNER
from static import *
from utils import check_spam_submissions
from utils import get_subreddit_by_name
from workingdata import WorkingData
from models.reddit_models.loggedactions import open_logged_action
from typing import Optional

def handle_dm_command(wd: WorkingData, subreddit_name: str, requestor_name, command, parameters) \
        -> tuple[str, bool]:
    subreddit_name: str = subreddit_name[2:] if subreddit_name.startswith('r/') else subreddit_name
    subreddit_name: str = subreddit_name[3:] if subreddit_name.startswith('/r/') else subreddit_name
    # subreddit_names = subreddit_name.split('+') if '+' in subreddit_name else [subreddit_name]  # allow
    if subreddit_name == MAIN_BOT_NAME or subreddit_name == wd.ri.bot_name:
        return "this command doesn't make sense", True

    command: str = command[1:] if command.startswith("$") else command

    result: tuple[Optional[TrackedSubreddit], str] = get_subreddit_by_name(wd, subreddit_name, update_if_due=False)
    tr_sub, req_status = result
    if not tr_sub:
        return f"Error retrieving information for /r/{subreddit_name} {req_status}", True
    try:

        moderators: List[str] = wd.ri.get_mod_list(subreddit=tr_sub)
    except (prawcore.exceptions.Redirect, prawcore.exceptions.Forbidden, prawcore.exceptions.NotFound):
        return f"Subreddit {subreddit_name} doesn't exist?", True
    print(f"asking for permission: {requestor_name}, mod list: {moderators}")
    if requestor_name is not None and requestor_name not in moderators and requestor_name != BOT_OWNER \
            and requestor_name != "[modmail]":
        if subreddit_name == "subredditname" or subreddit_name == "yoursubredditname":
            return "Please change 'subredditname' to the name of your subreddit so I know what subreddit you mean!", \
                   True
        return f"You do not have permission to do this. Are you sure you are a moderator of {subreddit_name}?\n\n " \
               f"/r/{subreddit_name} moderator list: {str(moderators)}, your name: {requestor_name}", True

    if tr_sub.bot_mod is None and requestor_name in moderators:
        tr_sub.bot_mod = requestor_name
        wd.s.add(tr_sub)
        wd.s.commit()

    if command in ['summary', 'unban', 'hallpass', 'blacklist', 'ban', 'ban-sc', 'ban-mc', 'ban-cf']:
        author_param = parameters[0] if parameters else None

        if not author_param:
            return "No author name given", True
        author_param = author_param.lower()
        author_param = author_param.replace('/u/', '')
        author_param = author_param.replace('u/', '')
        author_handle = wd.ri.reddit_client.redditor(author_param)
        if author_handle:
            try:
                _ = author_handle.id  # force load actual username capitalization
                author_param = author_handle.name
            except (prawcore.exceptions.NotFound, AttributeError):
                pass

        if command == 'summary':
            return tr_sub.get_author_summary(wd, author_param), True

        # Need to be an actual reddit author after this point
        if not author_handle:
            return "could not find that username `{}`".format(author_param), True

        if command in ['unban', 'hallpass', 'blacklist']:
            subreddit_author: SubAuthor = wd.s.query(SubAuthor).get((subreddit_name, author_handle.name))
            if not subreddit_author:
                subreddit_author = SubAuthor(tr_sub.subreddit_name, author_handle.name)

            if command == "unban":
                if subreddit_author.next_eligible and subreddit_author.next_eligible.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc):
                    subreddit_author.next_eligible = datetime(2019, 1, 1, 0, 0)
                    return_text = "User was removed from blacklist"
                else:
                    try:
                        wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).banned.remove(author_param)
                        return "Unban succeeded", False
                    except prawcore.exceptions.Forbidden:
                        return "Unban failed, I don't have permission in this subreddit to do that. Sorry.", True
                    except prawcore.exceptions.BadRequest:
                        return "The reddit server did not let me do this. Are they already unbanned?", True
            elif command == "hallpass":
                return_text = f"User {author_param} has been granted a hall pass. " \
                              "This means the next post by the user in this " \
                              "subreddit will not be automatically removed."
            elif command == "blacklist":
                subreddit_author.currently_blacklisted = True
                return_text = "User {} has been blacklisted from modmail. " \
                    .format(author_handle.name)
            else:
                return_text = "shouldn't get here?"
            wd.s.add(subreddit_author)
            return return_text, True

        ban_reason = " ".join(parameters[2:]) if parameters and len(parameters) >= 2 else None
        ban_note = "ModhelpfulBot: per modmail command"
        ban_length = int(parameters[1]) if parameters and len(parameters) >= 2 else None
        try:

            print(parameters, ban_length)
            if ban_length > 999 or ban_length < 1:
                return f"Invalid ban length: '{ban_length}'", True
        except ValueError:
            return f"Invalid ban length: '{ban_length}'", True

        try:
            if ban_length == 999 or ban_length is None:
                print("permanent ban", ban_length)
                wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                    author_param, note=ban_note, ban_message=ban_reason)
            else:
                print("non permanent ban", ban_length)
                wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).banned.add(
                    author_param, note=ban_note, ban_message=ban_reason,
                    duration=ban_length)
            return "Ban for {} was successful".format(author_param), True
        except prawcore.exceptions.Forbidden:
            return "Ban failed, I don't have permission to do that", True
        except praw.exceptions.RedditAPIException:
            return f"Ban failed, reddit reported {author_param} was not a valid user", True
    elif command == "showrules":
        lines = ["Rules for {}:".format(subreddit_name), ]
        rules = wd.ri.reddit_client.subreddit(subreddit_name).rules()['rules']
        for count, rule in enumerate(rules):
            lines.append("{}: {}".format(count + 1, rule['short_name']))
        return "\n\n".join(lines), True
    elif command == "stats":
        return tr_sub.get_sub_stats(), True
    elif command == "approve":
        submission_id = parameters[0] if parameters else None
        if not submission_id:
            return "No submission name given", True
        submission = wd.ri.reddit_client.submission(submission_id)
        if not submission:
            return "Cannot find that submission", True
        submission.mod.approve()
        return "Submission was approved.", False
    elif command == "remove":
        submission_id = parameters[0] if parameters else None
        if not submission_id:
            return "No author name given", True
        submission = wd.ri.reddit_client.submission(submission_id)
        if not submission:
            return "Cannot find that submission", True
        submission.mod.remove()
        return "Submission was removed.", True

    elif command == "citerule" or command == "testciterule":
        if not parameters:
            return "No rule # given", True
        try:
            # converting to integer
            rule_num = int(parameters[0]) - 1
        except ValueError:
            return "invalid rule #`{}`".format(parameters[0]), True
        rules = wd.ri.reddit_client.subreddit(subreddit_name).rules()['rules']
        rule = rules[rule_num] if rule_num < len(rules) else None
        if not rule:
            return "Invalid rule", True
        reply = "Please see rule #{}:\n\n>".format(rule_num + 1) + rule['short_name'].replace("\n\n", "\n\n>")
        internal = False if command == "citerule" else True
        return reply, internal
    elif command == "citerulelong" or command == "testciterulelong":
        if not parameters:
            return "No rule # given", True
        try:
            # converting to integer
            rule_num = int(parameters[0]) - 1
        except ValueError:
            return "invalid rule #`{}`".format(parameters[0]), True
        rules = wd.ri.reddit_client.subreddit(subreddit_name).rules()['rules']
        rule = rules[rule_num] if rule_num < len(rules) else None
        if not rule:
            return "Invalid rule", True
        reply = "Please see rule #{}:\n\n>".format(rule_num + 1) + rule['description'].replace("\n\n", "\n\n>")
        internal = False if command == "citerulelong" else True
        return reply, internal
    elif command == "canned" or command == "testcanned":
        if not parameters:
            return "No canned name given", True
        print(tr_sub.canned_responses)
        if parameters[0] not in tr_sub.canned_responses:
            return "no canned response by the name `{}`".format(parameters[0]), True
        reply = tr_sub.populate_tags2(tr_sub.canned_responses[parameters[0]])
        internal = False if command == "citerule" else True
        return reply, internal
    elif command == "reset":
        author_param = parameters[0] if parameters else None
        subreddit_author: SubAuthor = wd.s.query(SubAuthor).get((tr_sub.subreddit_name, author_param))
        if subreddit_author and subreddit_author.next_eligible.replace(tzinfo=timezone.utc) > datetime.now(pytz.utc):
            subreddit_author.next_eligible = datetime(2019, 1, 1, 0, 0)
            return "User was removed from blacklist", False
        posts = wd.s.query(SubmittedPost).filter(SubmittedPost.author == author_param,
                                                 # SubmittedPost.flagged_duplicate.is_(True),
                                                 SubmittedPost.counted_status_enum == CountedStatus.FLAGGED,
                                                 SubmittedPost.subreddit_name == tr_sub.subreddit_name).all()
        for post in posts:
            post.flagged_duplicate = False
            # post.counted_status = CountedStatus.EXEMPTED.value
            post.counted_status_enum = CountedStatus.EXEMPTED
            wd.s.add(post)
        wd.s.commit()
    elif command == "reloadconfig":
        wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).wiki.edit(
            wd.ri.bot_name,
            DEFAULT_CONFIG.replace("subredditname", tr_sub.subreddit_name).replace("moderatelyhelpfulbot",
                                                                                   wd.ri.bot_name),
            reason="reset to default_config")
        sub_info = wd.ri.get_subreddit_info(tr_sub.subreddit_name)
        tr_sub.update_from_subinfo(sub_info)
        _, _ = tr_sub.reload_yaml_settings()
        wd.s.add(tr_sub)
        wd.s.commit()

    elif command == "update":  # $update
        sub_info = wd.ri.get_subreddit_info(tr_sub.subreddit_name)
        tr_sub.update_from_subinfo(sub_info)
        worked, status = tr_sub.reload_yaml_settings()
        help_text = ""
        if "404" in status:
            help_text = f"This error means the wiki config page needs to be created. " \
                        f" See https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{MAIN_BOT_NAME}. "
        elif "403" in status:
            help_text = f"This error means the bot doesn't have enough permissions to view the wiki page. " \
                        f"Please make sure that you invited the bot to moderate and that the bot has " \
                        f"accepted the moderator invitation and give the bot wiki " \
                        f"privileges here: https://www.reddit.com/r/{tr_sub.subreddit_name}/about/moderators/ . " \
                        f"It is possible that the bot has not accepted the invitation due to current load.  " \
                        f"Link to your config: https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{MAIN_BOT_NAME}. "
        elif "yaml" in status:
            help_text = "Looks like there is an error in your yaml code. " \
                        "Please make sure to validate your syntax.  " \
                        f"Link to your config: https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{MAIN_BOT_NAME}. "
        elif "single document in the stream" in status:
            help_text = "Looks like there is an extra double hyphen in your code at the end, e.g. '--'. " \
                        "Please remove it.  " \
                        f"Link to your config: https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{MAIN_BOT_NAME}. "



        reply_text = f"Received message to update config for {subreddit_name}.  See the output below. {status}" \
                     f"Please message [/r/moderatelyhelpfulbot](https://www.reddit.com/" \
                     f"message/old?to=%2Fr%2Fmoderatelyhelpfulbot) if you have any questions \n\n" \
                     f"Update report: \n\n >{help_text}" \
                     f"\n\nCurrent Status: {tr_sub.active_status_enum}  "
        bot_owner_message = f"subreddit: {subreddit_name}\n\nrequestor: {requestor_name}\n\n" \
                            f"report: {status}\n\nCurrent Status: {tr_sub.active_status_enum}  "
        # wd.ri.reddit_client.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
        try:
            assert isinstance(requestor_name, str)
        except AssertionError:
            wd.ri.send_modmail(subreddit_name=wd.bot_name,body="Invalid user: bot_owner_message", use_same_thread=True)
            return "bad requestor", True
        if requestor_name and requestor_name.lower() != BOT_OWNER.lower():
            wd.ri.send_modmail(subreddit_name=wd.bot_name, body=bot_owner_message, use_same_thread=True)
        wd.s.add(tr_sub)
        wd.s.commit()
        wd.to_update_list = True
        return reply_text, True
    else:
        return "I did not understand that command", True


def handle_direct_messages(wd: WorkingData):
    print("checking direct messages")
    for message in wd.ri.reddit_client.inbox.unread(limit=None):
        import pprint
        pprint.pprint(message)

        logger.info(f"got this email {message.id} {message.author} {message.subject} {message.body}")

        new_action = open_logged_action(wd, message.subject, 'dm', message.id)
        if not new_action.is_new:
            continue

        # Get author name, message_id if available
        requestor_name = message.author.name if message.author else None

        message_id = wd.ri.reddit_client.comment(message.id).link_id if message.was_comment else message.name
        body_parts = message.body.split(' ')
        command = body_parts[0].lower() if len(body_parts) > 0 else None
        # subreddit_name = message.subject.replace("re: ", "") if command else None
        message_subject = message.subject.replace("re: ", "") if message.subject else None
        # First check if already actioned
        if check_actioned(wd, message_id):
            message.mark_read()  # should have already been "read"
            continue
        # Check if this a user mention (just ignore this)
        elif message_subject.startswith('username mention'):
            message.mark_read()
            continue
        elif message_subject.startswith('[Notification]'):
            message.mark_read()
        elif "has been removed as a moderator" in message_subject:
            message.mark_read()
            continue
        # Check if this a user mention (just ignore this)
        elif message_subject.startswith('moderator added'):
            message.mark_read()
            continue
        elif 'verification' in message.body or 'Verification' in message.body:
            message.mark_read()
            continue
        # Check if this is a ban notice (not new modmail)
        elif message_subject.startswith("re: You've been temporarily banned from participating"):
            message.mark_read()
            subreddit_name  = message_subject.replace("re: You've been temporarily banned from participating in r/", "")
            if not check_actioned(wd, "ban_note: {0}".format(requestor_name)):
                # record actioned first out of safety in case of error
                record_actioned(wd, "ban_note: {0}".format(message.author))

                tr_sub = TrackedSubreddit.get_subreddit_by_name(subreddit_name)
                if tr_sub and tr_sub.modmail_posts_reply and message.author:
                    try:
                        message.reply(body=tr_sub.get_author_summary(wd, message.author.name))
                    except (praw.exceptions.APIException, prawcore.exceptions.Forbidden):
                        pass
        # Respond to an invitation to moderate
        elif message_subject.startswith('invitation to moderate'):
            mod_mail_invitation_to_moderate(wd, message)
        elif command in ("summary", "update", "stats") or command.startswith("$"):
            subreddit_name = None
            thread_id = None
            is_modmail_message = hasattr(message, 'distinguished') and message.distinguished=='moderator'
            if requestor_name is None and not is_modmail_message:
                print("requestor name is none?")
                continue
            if is_modmail_message:
                #subreddit_name = message_subject
                thread_id = None
                requestor_name = "[modmail]"
            message_subject = message.subject
            if not subreddit_name:
                matches = \
                    re.match(r'^(re: )?(/?r/)?(?P<sub_name>[a-zA-Z0-9_]{1,21})(:(?P<thread_id>[a-z0-9]+))?$',
                             message_subject)
                if matches and matches.group("sub_name"):
                    subreddit_name = matches.group("sub_name")
                    # subject_parts = message.subject.replace("re: ", "").split(":")
                    if matches.group("thread_id"):
                        thread_id = matches.group("thread_id")
                    # subreddit_name = subject_parts[0].lower().replace("re: ", "").replace("/r/", "").replace("r/", "")
            print(f"Subreddit name = {subreddit_name}")
            if not subreddit_name or not subreddit_name.replace('_','').isalnum()  \
                    or '/' in subreddit_name or len(subreddit_name) > 21 or subreddit_name == "yoursubredditname":
                message.mark_read()
                message.reply(body=f"Sorry, I don''t think '{message_subject}' contains a valid subreddit?")
                continue
            result: tuple[Optional[TrackedSubreddit], str] = get_subreddit_by_name(wd, subreddit_name)
            tr_sub, response = result
            if tr_sub:
                response, _ = handle_dm_command(wd, subreddit_name, requestor_name, command, body_parts[1:])
            if tr_sub and thread_id:
                wd.ri.send_modmail(subreddit=tr_sub, body=response[:9999], thread_id=thread_id)
            else:

                message.reply(body=response[:9999])
            bot_owner_message = f"subreddit: {subreddit_name}\n\n" \
                                f"requestor: {requestor_name}\n\n" \
                                f"command: {command}\n\n" \
                                f"response: {response}\n\n" \
                                f"wiki: https://www.reddit.com/r/{subreddit_name}/wiki/{MAIN_BOT_NAME}\n\n"
            # if requestor_name is not None and requestor_name.lower() != BOT_OWNER.lower():
            #    wd.ri.send_modmail(subreddit_name=MAIN_BOT_NAME, subject="[Notification]  Command processed",
            #                       body=bot_owner_message, use_same_thread=True)
            # wd.ri.reddit_client.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)

        elif requestor_name and not check_actioned(wd, requestor_name):
            record_actioned(wd, requestor_name)
            message.mark_read()
            try:
                # ignore profanity
                if "fuck" in message.body:
                    continue
                message.reply(body="Hi, thank you for messaging me! "
                                   "I am a non-sentient bot, and I act only in the accordance of the rules set by the "
                                   "moderators "
                                   "of the subreddit. Unfortunately, I am unable to answer or direct requests. Please "
                                   "see this [link](https://www.reddit.com/r/SolariaHues/comments/mz7zdp/)")
                import pprint

                # assume you have a Reddit instance bound to variable `reddit`

                # print(submission.title)  # to make it non-lazy
                pprint.pprint(vars(message))
            except prawcore.exceptions.Forbidden:
                pass
            except praw.exceptions.APIException:
                pass

        message.mark_read()
        record_actioned(wd, message_id)
    wd.s.commit()


def mod_mail_invitation_to_moderate(wd: WorkingData, message):
    subreddit_name = message.subject.replace("re: invitation to moderate /r/", "")
    subreddit_name = subreddit_name.replace("invitation to moderate /r/", "")
    result: tuple[Optional[TrackedSubreddit], str] = get_subreddit_by_name(wd, subreddit_name,  create_if_not_exist=False)
    tr_sub, req_status = result
    # accept invite if accepting invites or had been accepted previously
    print(f"Got invite for {subreddit_name}, {tr_sub}, accepting new subs?{ACCEPTING_NEW_SUBS}")
    if tr_sub or (ACCEPTING_NEW_SUBS and 'karma' not in subreddit_name.lower()):
        try:
            wd.ri.reddit_client.subreddit(subreddit_name).mod.accept_invite()
        except (praw.exceptions.RedditAPIException, prawcore.exceptions.ServerError) as ex:  # Changed from praw.exceptions.APIException
            reply =  f"Message from reddit: {ex.message}"
            print(f"error reply {reply}")
            message.reply(body=reply)
            message.mark_read()

        if not tr_sub:
            result: tuple[Optional[TrackedSubreddit], str] = get_subreddit_by_name(wd, subreddit_name,
                                                                                   create_if_not_exist=False)
            tr_sub, req_status = result

        message.reply(
            body=f"Hi, thank you for inviting me!  I will start working now. Please make sure I have a config. "
                 f"I will try to create one at https://www.reddit.com/r/{subreddit_name}/wiki/{wd.bot_name} . "
                 f"You may need to create it. You can find examples at "
                 f"https://www.reddit.com/r/{wd.bot_name}/wiki/index . ")
        try:
            sub_info = wd.ri.get_subreddit_info(tr_sub.subreddit_name)
            access_status = sub_info.check_sub_access(wd.ri, ignore_no_mod_access=True)
            if access_status == SubStatus.NO_CONFIG:
                logger.warning(f'no wiki page {tr_sub.subreddit_name}..will create')
                wd.ri.reddit_client.subreddit(tr_sub.subreddit_name).wiki.create(
                    wd.bot_name, DEFAULT_CONFIG.replace("subredditname", tr_sub.subreddit_name).replace("moderatelyhelpfulbot", wd.bot_name),
                    reason="default_config"
                )

                wd.ri.send_modmail(wd, subject=f"[Notification] Config created",
                                    body=f"There was no configuration created for {wd.bot_name} so "
                                         "one was automatically generated. Please check it to make sure it is "
                                         f"what you want. https://www.reddit.com/r/{tr_sub.subreddit_name}/wiki/{wd.bot_name}")
                tr_sub.active_status_enum = SubStatus.ACTIVE
                wd.s.add(tr_sub)
        except prawcore.exceptions.NotFound:
            logger.warning(f'no config accessible for {tr_sub.subreddit_name}')
            tr_sub.active_status_enum = SubStatus.CONFIG_ACCESS_ERROR
            wd.s.add(tr_sub)
    else:
        try:
            message.reply(body=f"Invitation received. Please wait for approval by bot owner. In the mean time, "
                               f"you may create a config at https://www.reddit.com/r/{subreddit_name}/wiki/{wd.bot_name} .")
        except (praw.exceptions.RedditAPIException, prawcore.exceptions.ServerError) as ex:
            message.mark_read()
    message.mark_read()


def handle_modmail_message(wd: WorkingData, convo):
    # Ignore old messages past 24h
    if iso8601.parse_date(convo.last_updated) < datetime.now(timezone.utc) - timedelta(hours=24):
        convo.read()

        return

    initiating_author_name = convo.authors[0].name  # praw query
    subreddit_name = convo.owner.display_name  # praw query
    result: tuple[Optional[TrackedSubreddit], str] = get_subreddit_by_name(wd, subreddit_name,  create_if_not_exist=True, update_if_due=True)
    tr_sub, req_status = result
    if not tr_sub:
        return

    # print(f"catching convoid {convo.id} {initiating_author_name}")
    if not tr_sub.mm_convo_id and initiating_author_name and initiating_author_name.lower() == wd.bot_name.lower():
        tr_sub.mm_convo_id = convo.id
        wd.s.add(tr_sub)
        wd.s.commit()
        convo.read()

    import pprint
    pprint.pprint(convo)
    new_action = open_logged_action(wd, subreddit_name, 'mm', f"{convo.id}-{convo.num_messages}")
    if not new_action.is_new:
        return

    #ignore verification modmails
    if "verification" in convo.subject:
        return


    # Ignore if already actioned (at this many message #s)
    if check_actioned(wd, "mm{}-{}".format(convo.id, convo.num_messages)):  # sql query
        try:
            convo.read()  # praw query
            # convo.read()  # praw query24
        except prawcore.exceptions.Forbidden:
            pass

        return

    if initiating_author_name:
        subreddit_author: SubAuthor = wd.s.query(SubAuthor).get((subreddit_name, initiating_author_name))
        if subreddit_author and subreddit_author.currently_blacklisted:
            convo.reply("This author is modmail-blacklisted", internal=True)
            convo.archive()

            return

    response = None
    response_internal = False
    command = "no_command"
    debug_notify = False
    last_post = None

    # If this conversation only has one message -> canned response or summary table
    # Does not respond if already responded to by a mod
    if convo.num_messages == 1 \
            and initiating_author_name.lower() not in tr_sub.mod_list.lower() \
            and initiating_author_name not in ("AutoModerator", "Sub_Mentions", "mod_mailer")\
            and initiating_author_name.lower() != wd.bot_name.lower()\
            and initiating_author_name.lower() != MAIN_BOT_NAME:
        # Join request
        if "ADD USER" in convo.messages[0].body:
            record_actioned(wd, "mm{}-{}".format(convo.id, convo.num_messages))
            return

        import re
        submission = None
        urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
        if len(urls) == 2:  # both link and link description
            print(f"found url: {urls[0][1]}")
            submission = wd.ri.reddit_client.submission(urls[0][1])

        # Automated approval (used for new account screening - like in /r/dating)
        if tr_sub.modmail_auto_approve_messages_with_links:

            urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
            if submission:
                try:
                    in_submission_urls = re.findall(LINK_REGEX, submission.selftext)
                    bad_words = "Raya", 'raya', 'dating app'
                    if not in_submission_urls and 'http' not in submission.selftext \
                            and submission.banned_by and submission.banned_by == "AutoModerator" \
                            and not any(bad_word in submission.selftext for bad_word in bad_words):
                        submission.mod.approve()
                        response = "Since you contacted the mods this bot " \
                                   "has approved your post on a preliminary basis. " \
                                   " The subreddit moderators may override this decision, however\n\n Your text:\n\n>" \
                                   + submission.selftext.replace("\n\n", "\n\n>")
                except prawcore.exceptions.NotFound:
                    pass

        # Non-ALL canned reply - more personalized reply
        if not response:
            # first check if any posts exist for person
            recent_posts: List[SubmittedPost] = wd.s.query(SubmittedPost) \
                .filter(SubmittedPost.subreddit_name.ilike(subreddit_name)) \
                .filter(SubmittedPost.author == initiating_author_name).all()
            removal_reason = None
            # Check again if still no posts in database
            if not recent_posts:
                check_spam_submissions(wd, sub_list=subreddit_name)
                recent_posts: List[SubmittedPost] = wd.s.query(SubmittedPost) \
                    .filter(SubmittedPost.subreddit_name.ilike(subreddit_name)) \
                    .filter(SubmittedPost.author == initiating_author_name).all()
            # Collect removal reason if possible from bot comment or mod comment
            last_post = None
            if submission:
                last_post = wd.s.query(SubmittedPost).get(submission.id)
                if last_post:
                    last_post.api_handle = submission
            if recent_posts:
                last_post = recent_posts[-1] if not last_post else last_post
                # if removal reason hasn't been pulled, try pulling again
                if not last_post.bot_comment_id:
                    posted_status = wd.ri.get_posted_status(last_post, get_removed_info=True)
                    # update db if found an explanation
                    if last_post.bot_comment_id:  # found it now?
                        wd.s.add(last_post)
                        wd.s.commit()
                        removal_reason = wd.ri.get_removed_explanation(last_post)  # try to get removal reason
                        if removal_reason:
                            removal_reason = removal_reason.replace("\n\n", "\n\n>")
                            removal_reason = f"-------------------------------------------------\n\n{removal_reason}"
                    if not removal_reason:  # still couldn't find removal reason, just use posted status
                        removal_reason = f"status: {posted_status.value}\n\n " \
                                         f"flair: {wd.ri.get_submission_api_handle(last_post).link_flair_text}"
                        if posted_status == PostedStatus.SPAM_FLT:
                            removal_reason += " \n\nThis means the Reddit spam filter thought your post was spam " \
                                              "and it was NOT removed by the subreddit moderators.  You can try " \
                                              "verifying your email and building up karma to avoid the spam filter." \
                                              "There is more information here: " \
                                              "https://www.reddit.com/r/NewToReddit/wiki/ntr-guidetoreddit"
                        r_last_post = wd.ri.get_submission_api_handle(last_post)
                        if hasattr(r_last_post, 'removal_reason') and r_last_post.removal_reason \
                                and not posted_status == posted_status.UP:
                            removal_reason += f"\n\n{r_last_post.removal_reason}"

            # All reply if specified
            if not response and tr_sub.modmail_all_reply and tr_sub.modmail_all_reply is not True:
                # response = populate_tags(tr_sub.modmail_all_reply, None, tr_sub=tr_sub, prev_posts=recent_posts)
                response = tr_sub.populate_tags2(tr_sub.modmail_all_reply, post_list=recent_posts)
            # No links auto reply if specified
            if not response and tr_sub.modmail_no_link_reply:
                import re
                urls = re.findall(REDDIT_LINK_REGEX, convo.messages[0].body)
                if len(urls) < 2:  # both link and link description
                    # response = populate_tags(tr_sub.modmail_no_link_reply, None, tr_sub=tr_sub,
                    # prev_posts=recent_posts)
                    response = tr_sub.populate_tags2(tr_sub.modmail_no_link_reply, post_list=recent_posts)
                    # Add last found link
                    if recent_posts:
                        response += f"\n\nAre you by chance referring to this post? " \
                                    f"{recent_posts[-1].get_comments_url()}"
                        # Add removal reason if found
                        if removal_reason:
                            response += f"\n\nIf so, does this answer your question?\n\n>{removal_reason}"
                    # debug_notify = True

            # Having previous posts reply
            if not response and recent_posts and tr_sub.modmail_posts_reply:
                # Does have recent posts -> reply with posts reply
                if tr_sub.modmail_posts_reply is True:  # default -> only goes to internal
                    response = ">" + convo.messages[0].body_markdown.replace("\n\n", "\n\n>")
                    if removal_reason and tr_sub.modmail_removal_reason_helper:
                        # response += "\n\n-------------------------------------------------"
                        # response += f"\n\nRemoval reason from [post]
                        # ({last_post.get_comments_url()}):\n\n {removal_reason})"
                        # response += "\n\n-------------------------------------------------"
                        non_internal_response = f"AUTOMATED RESPONSE with reference information " \
                                                f"(please ignore unless relevant):\n\n " \
                                                f"last post: [{last_post.title}]({last_post.get_comments_url()})\n\n" \
                                                f"{removal_reason}"
                        if "status:mod-removed" not in non_internal_response \
                                and "status: AutoMod-removed" not in non_internal_response:
                            # don't answer if not particularly helpful
                            convo.reply(non_internal_response, internal=False)
                    smart_link = f"https://old.reddit.com/message/compose?to={wd.bot_name}" \
                                 f"&subject={subreddit_name}:{convo.id}" \
                                 f"&message="
                    response += tr_sub.populate_tags2(
                        "\n\n{summary table}\n\n"
                        f"Available 'smart links': | "
                        f"[$update]({smart_link}$update) | "
                        f"[$summary {initiating_author_name}]({smart_link}$summary {initiating_author_name}) | "
                        f"[$hallpass {initiating_author_name}]({smart_link}$hallpass {initiating_author_name}) | "
                        # f"[$ban {initiating_author_name}]({smart_link}$ban {initiating_author_name} length reason) | "   
                        # need to fix parameters, length reason
                        f"[$unban {initiating_author_name}]({smart_link}$unban {initiating_author_name}) | "
                        f"[$approve {last_post.id}]({smart_link}$approve {last_post.id}) | "
                        f"[$remove {last_post.id}]({smart_link}$remove {last_post.id}) | "
                        f"\n\nDO NOT CLICK ON LinkedIn LINKS OR URL shorteners - they have been used to dox moderators."
                        f"\n\nPlease subscribe to /r/ModeratelyHelpfulBot for updates.\n\n", None,
                        post_list=recent_posts)

                    response_internal = True
                # Reply using a specified template
                else:  # given a response to say -> not internal
                    # response = populate_tags(tr_sub.modmail_posts_reply, None, prev_posts=recent_posts)
                    response = tr_sub.populate_tags2(tr_sub.modmail_posts_reply, post_list=recent_posts)

            # No posts reply
            elif not response and not recent_posts and tr_sub.modmail_no_posts_reply:
                response = ">" + convo.messages[0].body_markdown.replace("\n\n", "\n\n>") + "\n\n\n\n"
                # response += populate_tags(tr_sub.modmail_no_posts_reply, None, tr_sub=tr_sub)
                response += tr_sub.populate_tags2(tr_sub.modmail_no_posts_reply)
                response_internal = tr_sub.modmail_no_posts_reply_internal
            # debug_notify = True
    else:
        # look for commands if message count >1 and message by mod
        last_author_name: str = convo.messages[-1].author.name
        last_message = convo.messages[-1]
        body_parts: List[str] = last_message.body_markdown.split(' ')
        command: str = body_parts[0].lower() if len(body_parts) > 0 else None
        if last_author_name != wd.bot_name and last_author_name in tr_sub.subreddit_mods:
            # check if forgot to reply as the subreddit
            if command.startswith("$") or command in ('summary', 'update'):
                response, response_internal = handle_dm_command(wd, subreddit_name, last_author_name, command,
                                                                body_parts[1:])
            # Catch messages that weren't meant to be internal
            """  doesn't work anymore?
            elif convo.num_messages > 2 and convo.messages[-2].author.name == wd.bot_name and last_message.is_internal:
                if not check_actioned(f"ic-{convo.id}") and tr_sub.modmail_notify_replied_internal:
                    response = "Hey sorry to bug you, but was this last message not meant to be moderator-only?  " \
                               f"https://mod.reddit.com/mail/perma/{convo.id} \n\n" \
                               "Set `modmail_notify_replied_internal: false` to disable this message"

                    response_internal = True
                    record_actioned(wd, f"ic-{convo.id}")
                    tr_sub.send_modmail(
                        subject="[Notification] Possible moderator-only reply not meant to be moderator-only",
                        body=response)
                    response = None
            """
    if response:
        try:
            convo.reply(body=tr_sub.populate_tags2(response[0:9999], recent_post=last_post), internal=response_internal)

            bot_owner_message = f"subreddit: {subreddit_name}\n\nresponse:\n\n{response}\n\n" \
                                f"https://mod.reddit.com/mail/all/{convo.id}"[0:9999]

            if debug_notify:
                # wd.ri.reddit_client.redditor(BOT_OWNER).message(subreddit_name, bot_owner_message)
                wd.ri.send_modmail(subreddit_name=wd.bot_name, subject="[Notification] MHB Command used",
                                   body=bot_owner_message, use_same_thread=True)
        except (prawcore.exceptions.BadRequest, praw.exceptions.RedditAPIException, prawcore.exceptions.ServerError):
            logger.debug(f"reply failed {subreddit_name} {response} ")
    record_actioned(wd, f"mm{convo.id}-{convo.num_messages}")
    convo.read()
    wd.s.commit()


def handle_modmail_messages(wd: WorkingData):
    print("checking modmail")

    for convo in wd.ri.reddit_client.subreddit('all').modmail.conversations(state="mod", sort='unread', limit=15):
        handle_modmail_message(wd, convo=convo)

    for convo in wd.ri.reddit_client.subreddit('all').modmail.conversations(state="join_requests", sort='unread',
                                                                            limit=15):
        handle_modmail_message(wd, convo=convo)

    for convo in wd.ri.reddit_client.subreddit('all').modmail.conversations(state="all", sort='unread', limit=15):
        handle_modmail_message(wd, convo=convo)


def check_actioned(wd: WorkingData, comment_id: str):
    response: ActionedComments = wd.s.query(ActionedComments).get(comment_id)
    if response:
        return True
    return False


def record_actioned(wd: WorkingData, comment_id: str):
    response: ActionedComments = wd.s.query(ActionedComments).get(comment_id)
    if response:
        return
    wd.s.add(ActionedComments(comment_id))


