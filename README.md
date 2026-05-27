# ModeratelyHelpfulBot (Devvit Edition)

## A Modern, Serverless Port of the Legacy ModeratelyHelpfulBot

This application is a complete, modernized Devvit port of the popular Python/PRAW moderation bot, **ModeratelyHelpfulBot**.

The legacy version required subreddit moderators to run a continuous background Python script on a dedicated server to poll for new posts. By porting this architecture to Reddit's native Devvit platform, this modernized version operates on an **event-driven, serverless architecture**. It wakes up instantly when a post is submitted (`onPostSubmit`), evaluates the user's history using a dedicated Redis database and takes action with zero hosting costs, zero API polling delays and no need for moderators to edit raw YAML configuration files.

## Core Features & Configurable Notifications

This port captures the strict enforcement capabilities of the original daemon while vastly improving the user experience for the moderation team.

* **Smart Post Rate Limiting:** Enforces strict frequency limits (e.g., maximum 2 posts every 72 hours) and tracks users dynamically over time.
* **Rolling Time Windows:** Evaluates user quotas on a true rolling chronological basis, automatically expiring old posts.
* **Historical State Validation (No "Zombie" Penalties):** Intelligently ignores posts that were deleted by the user within the grace period, or posts that were instantly filtered by AutoModerator/human mods. Users are only penalized for active, live posts.
* **Automated Escalation (Auto-Banning):** Tracks repeat offenders and can issue automated temporary or permanent bans after a configurable number of strikes.
* **Modmail Gatekeeping:** Intercepts incoming modmails. If a user with no logged active posts sends a modmail, the bot auto-replies asking for a link, saving moderators time.
* **Granular Notification Configurability:** Moderators have total control over how users are notified of removals:
* **Silent Analytics:** Silently log the removal to native Reddit Mod Tools by attaching a specific Subreddit Removal Reason ID.
* **Dynamic Stickied Comments:** Drop a distinguished, stickied comment injecting live variables (e.g., exactly how many posts the user made and the timeframe) so the user understands the math behind their removal.
* **Modmail Notifications:** Send a private notification via the subreddit's modmail.
* **Hybrid Approach:** Moderators can configure the bot to utilize any combination of the above simultaneously.



## Configurable Settings

Configuration is handled entirely through Reddit's native UI, utilizing clean toggle switches, dropdowns, and grouped fields to prevent configuration fatigue.

**Basic Quota Limits**

* **Maximum posts allowed per interval** (Default: 1)
* **Time interval** (in hours) (Default: 72)
* **Grace period** (in minutes): A window where users can delete a mistake and repost without it counting against their quota. (Default: 30)

**Enforcement Actions & Notifications**

* **Action to take on violation:** Choose between removing the post or reporting it to the mod queue.
* **Notify user via:** Dropdown to select Comment, Modmail, Both, or None.
* **Removal Message Template:** Fully customizable text field supporting dynamic variables (`{author}`, `{maxcount}`, `{interval}`, `{subreddit}`).

**Auto-Ban Configuration**

* **Ban Threshold:** The number of violations required before issuing an automated ban (Set to 0 to disable).
* **Violation Window:** How many days a strike stays on a user's record before being forgiven.
* **Ban Duration:** Configurable in days (Set to 999 for a permanent ban).

**Exemptions & Filters**

* **Exempt Moderator Posts:** Ignore posts submitted by the mod team.
* **Exempt Text (Self) / Link Posts:** Apply rate limits strictly to specific post formats.
* **Keyword Exemptions:** Whitelist posts automatically if they contain specific Title Keywords or Author Flair Keywords.
* **Active State Validation:** Toggle whether posts previously removed by AutoModerator or human moderators count toward the user's limit.
* **Lock Thread:** Automatically lock the comments of removed offending posts.

## Interactive Mod Actions

Beyond automated background tasks, this app provides real-time moderation tools directly inside the Reddit UI via custom Mod Menu buttons.

* **Check MHB Status (Post Menu):** Instantly scans the author of a specific post, applies the rolling-window math, and displays a toast notification showing their exact current quota (e.g., *u/username has 2/3 active posts in the last 72h*).
* **Grant MHB Hallpass (Post Menu):** Allows a moderator to manually whitelist a user. Their very next post will completely bypass the frequency limit and consume the hallpass.
* **MHB: Configure Removal Reason (Subreddit Menu):** Opens a custom, dynamic Devvit form that queries the subreddit's actual, live rules. Allows moderators to select a specific native Removal Reason from a clean dropdown, which the bot will seamlessly attach to future removals for perfect mod log analytics.

## Unimplemented Legacy Features

While this Devvit app ports the vast majority of the original `ModeratelyHelpfulBot` feature set, a few specific functionalities were intentionally left behind due to the sandboxed nature of Devvit:

* **NSFW Monitoring:** The original PRAW bot utilized a separate `nsfw_monitoring.py` script that scraped user data to calculate their NSFW percentage and flag posts. This feature is currently not available.
* **Cross-Subreddit Centralized Tracking:** The legacy bot operated as a single, centralized account that could track a user's spam footprint across hundreds of different subreddits. Devvit apps (and their Redis databases) are completely isolated. This app tracks user quotas flawlessly, but *only* within the boundaries of the specific subreddit it is installed in.

## Conclusion
 
While this Devvit port shifts the underlying architecture to a serverless model, it stands entirely on the shoulders of the original ModeratelyHelpfulBot. The goal here wasn't to reinvent the wheel, but simply to offer moderation teams an accessible alternative that runs natively within Reddit, eliminating the need to maintain external servers or troubleshoot Python scripts.

Hopefully, by lowering the technical barrier to entry, this tool will make managing community rate limits just a little bit easier and less stressful for volunteer mod teams.