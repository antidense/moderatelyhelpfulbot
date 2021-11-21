DEFAULT_CONFIG = """---
###### If you edit this page, you must [click this link, then click "send"](https://old.reddit.com/message/compose?to=moderatelyhelpfulbot&subject=subredditname&message=update) to have the bot update
######https://www.reddit.com/r/moderatelyhelpfulbot/wiki/index

###### [User Summary, click this link](https://www.reddit.com/message/compose?to=moderatelyhelpfulbot&subject=subredditname&message=$summary u/username) - Will show you the users posts

###### [User Extra Post, click this link](https://www.reddit.com/message/compose?to=moderatelyhelpfulbot&subject=subredditname&message=$hallpass u/username)  - Will allow the user one extra post
post_restriction: 
    max_count_per_interval: 1
    min_post_interval_hrs: 72
    action: remove
    ban_threshold_count: 5
    ban_duration_days: ~
    comment: "Hello and thank you for posting to {subreddit}! It seems you have previously posted {maxcount} submission within {interval}, so your post has been removed as per the post frequency rule.  If you believe your post has been removed by mistake please [message the moderators](https://old.reddit.com/message/compose?to=%2Fr%2F{subreddit}).\n"
    distinguish: true
    grace_period_mins: 60
    ignore_AutoModerator_removed: true
    ignore_moderator_removed: true
    title_exempt_keyword: Modpost
modmail: 
    modmail_all_reply: ~
    modmail_no_posts_reply: "Hello, and thank you for your message. I could not find any prior posts from you. If you have a particular question about a post, please reply with a link to the post!\n"
    modmail_no_posts_reply_internal: false
    modmail_posts_reply: ~
"""


NAFMC = (
    "Per our rules, contacting minors while having a history of NSFW comments and/or posts is a bannable offense.  "
    "Your account was reviewed by a mod team and determined to be non-compliant with our rules."
)

NAFSC = (
    "Per recent community feedback, we are temp banning anyone with a history that is more than "
    "80% NSFW to protect minors and reduce sexual harassment in our subreddit.  "
    "Please get this down if you wish to continue to participate here. "
    "Your score is currently {NSFWPCT}% and is recalculated weekly."
)

NAFCF = f"Per our rules, catfishing -- identifying as different ages in different posts -- is a bannable offense."
