import { Devvit } from '@devvit/public-api';
import { appSettings } from './settings.js';
import {
    getUserPostTimestamps,
    saveUserPostTimestamps,
    recordAndCountViolations,
    hasHallpass,
    consumeHallpass,
    grantHallpass
} from './redis.js';

Devvit.configure({
    redditAPI: true,
    redis: true,
});

Devvit.addSettings(appSettings);

const removalReasonForm = Devvit.createForm(
    (data: any) => ({
        title: 'Configure MHB Removal Reason',
        acceptLabel: 'Save',
        fields: [
            {
                type: 'select',
                name: 'selectedReasonId',
                label: 'Select a Rule/Removal Reason to attach when MHB removes a post:',
                options: data.reasons || [],
                multiSelect: false,
            }
        ]
    }),
    async (event, context) => {
        const { selectedReasonId } = event.values;
        if (selectedReasonId && selectedReasonId.length > 0) {
            const id = selectedReasonId[0];
            const subreddit = await context.reddit.getCurrentSubreddit();
            await context.redis.set(`mhb_removal_reason_id:${subreddit.id}`, id);
            context.ui.showToast('Removal Reason configured successfully! MHB will now use this reason.');
        } else {
            const subreddit = await context.reddit.getCurrentSubreddit();
            await context.redis.del(`mhb_removal_reason_id:${subreddit.id}`);
            context.ui.showToast('Cleared Removal Reason configuration.');
        }
    }
);

Devvit.addMenuItem({
    location: 'subreddit',
    label: 'MHB: Configure Removal Reason',
    forUserType: 'moderator',
    onPress: async (event, context) => {
        const subreddit = await context.reddit.getCurrentSubreddit();
        const reasons = await context.reddit.getSubredditRemovalReasons(subreddit.name);
        
        if (!reasons || reasons.length === 0) {
            context.ui.showToast('This subreddit has no native removal reasons configured.');
            return;
        }

        const options = reasons.map((r: any) => ({ label: r.title, value: r.id }));
        
        // Add a 'None' option to clear it
        options.unshift({ label: '-- None (Do not attach a reason) --', value: '' });

        context.ui.showForm(removalReasonForm, { reasons: options });
    }
});

// Helper to format string template
function formatMessage(template: string, replacements: Record<string, string>): string {
    let result = template;
    for (const [key, value] of Object.entries(replacements)) {
        result = result.replace(new RegExp(`{${key}}`, 'g'), value);
    }
    return result;
}

// Helper to extract active, non-removed historical posts
async function getActiveUserPosts(redis: any, reddit: any, subredditId: string, username: string, cutoffTime: number, gracePeriodMins: number, ignoreAutoMod: boolean, ignoreMod: boolean) {
    let history = await getUserPostTimestamps(redis, subredditId, username);
    const originalLength = history.length;
    history = history.filter(record => record.timestamp > cutoffTime);
    
    const activeRecords = [];
    const now = Date.now();
    for (const record of history) {
        if (record.id === 'unknown') {
            activeRecords.push(record);
            continue;
        }
        try {
            const pastPost = await reddit.getPostById(record.id);
            if (pastPost.isRemoved() || pastPost.isSpam()) {
                const category = pastPost.removedByCategory;
                if (category === 'automod_filtered' && !ignoreAutoMod) {
                    console.log(`[MHB] Counting AutoMod-removed historical post ${record.id} due to settings`);
                    activeRecords.push(record);
                    continue;
                }
                if (category === 'moderator' && !ignoreMod) {
                    console.log(`[MHB] Counting Mod-removed historical post ${record.id} due to settings`);
                    activeRecords.push(record);
                    continue;
                }
                console.log(`[MHB] Ignoring historical post ${record.id} (Removed by ${category || 'Unknown'})`);
                continue;
            }
            if (pastPost.authorName === '[deleted]') {
                throw new Error('User deleted zombie post');
            }
            activeRecords.push(record);
        } catch (e) {
            const msSincePost = now - record.timestamp;
            if (msSincePost <= gracePeriodMins * 60 * 1000) {
                console.log(`[MHB] Ignoring historical post ${record.id} (User deleted within ${gracePeriodMins}m grace period)`);
                continue;
            }
            console.log(`[MHB] Counting historical post ${record.id} (User deleted AFTER grace period)`);
            activeRecords.push(record);
        }
    }
    return { activeRecords, originalLength, pruned: originalLength - history.length };
}

Devvit.addTrigger({
    event: 'PostSubmit',
    onEvent: async (event, context) => {
        const { reddit, redis, settings } = context;
        if (!event.post || !event.author) return;
        
        console.log(`[MHB] New post submitted by u/${event.author.name} (Post ID: ${event.post.id})`);

        const subreddit = await reddit.getCurrentSubreddit();
        const author = await reddit.getUserById(event.author.id);
        if (!author) return;

        // Load Settings
        const maxPosts = await settings.get<number>('maxPostsPerInterval') ?? 1;
        const intervalHours = await settings.get<number>('intervalHours') ?? 72;
        const actionToTake = await settings.get<string[]>('actionToTake') ?? ['remove'];
        const exemptModerator = await settings.get<boolean>('exemptModeratorPosts') ?? true;
        const exemptText = await settings.get<boolean>('exemptTextPosts') ?? false;
        const exemptLink = await settings.get<boolean>('exemptLinkPosts') ?? false;
        const exemptFlairKeyword = await settings.get<string>('exemptFlairKeyword') ?? '';
        const exemptTitleKeyword = await settings.get<string>('exemptTitleKeyword') ?? '';
        
        const post = await reddit.getPostById(event.post.id);

        // Check exemptions (Fast synchronous first)
        if (post.isRemoved() || post.isSpam()) {
            console.log(`[MHB] Ignoring post ${post.id}: Already removed or spam.`);
            return;
        }
        
        const isTextPost = !post.url || post.url.includes(subreddit.name);
        if (exemptText && isTextPost) {
            console.log(`[MHB] Ignoring post ${post.id}: Text posts are exempt.`);
            return;
        }
        if (exemptLink && !isTextPost) {
            console.log(`[MHB] Ignoring post ${post.id}: Link posts are exempt.`);
            return;
        }

        if (exemptFlairKeyword && event.post.authorFlair?.text?.includes(exemptFlairKeyword)) {
            console.log(`[MHB] Ignoring post ${post.id}: Flair matches exempt keyword.`);
            return;
        }
        if (exemptTitleKeyword && event.post.title.includes(exemptTitleKeyword)) {
            console.log(`[MHB] Ignoring post ${post.id}: Title matches exempt keyword.`);
            return;
        }
        
        // Slower API checks
        if (exemptModerator) {
            const mods = await subreddit.getModerators({ limit: 100 }).all();
            const isMod = mods.some(mod => mod.id === author.id);
            if (isMod) {
                console.log(`[MHB] Ignoring post ${post.id}: Author is a moderator.`);
                return;
            }
        }

        // Check Hallpass
        if (await hasHallpass(redis, subreddit.id, author.username)) {
            await consumeHallpass(redis, subreddit.id, author.username);
            console.log(`User ${author.username} used a hallpass on post ${post.id}.`);
            return;
        }

        const gracePeriodMins = await settings.get<number>('gracePeriodMins') ?? 30;
        const ignoreAutoMod = await settings.get<boolean>('ignoreAutoModeratorRemoved') ?? true;
        const ignoreMod = await settings.get<boolean>('ignoreModeratorRemoved') ?? true;
        const shouldLockThread = await settings.get<boolean>('lockThread') ?? true;
        
        const now = Date.now();
        const intervalMs = intervalHours * 60 * 60 * 1000;
        const cutoffTime = now - intervalMs;

        // Get and validate history
        const { activeRecords, originalLength, pruned } = await getActiveUserPosts(redis, reddit, subreddit.id, author.username, cutoffTime, gracePeriodMins, ignoreAutoMod, ignoreMod);
        
        console.log(`[MHB] u/${author.username} history: ${activeRecords.length} active posts in window (pruned ${pruned} expired). Limit is ${maxPosts}.`);
        
        if (activeRecords.length >= maxPosts) {
            console.log(`[MHB] VIOLATION: u/${author.username} exceeded limit of ${maxPosts} posts per ${intervalHours} hours.`);
            // Violation!
            if (actionToTake.includes('remove')) {
                await post.remove(false); 
                
                if (shouldLockThread) {
                    await post.lock();
                    console.log(`[MHB] Locked offending post.`);
                }
                
                // Add Removal Note (if ID provided)
                const reasonId = await redis.get(`mhb_removal_reason_id:${subreddit.id}`);
                if (reasonId && reasonId.trim() !== '') {
                    try {
                        await post.addRemovalNote({ reasonId: reasonId.trim() });
                        console.log(`[MHB] Attached removal reason ID: ${reasonId}`);
                    } catch (e) {
                        console.error(`[MHB] Failed to attach removal reason ID ${reasonId}`, e);
                    }
                }
                
                // Send notifications based on preferences
                const notifyMethods = await settings.get<string[]>('removalNotificationMethod') ?? ['comment'];
                const notifyMethod = notifyMethods[0] || 'comment';
                
                if (notifyMethod !== 'none') {
                    const template = await settings.get<string>('removalCommentTemplate') ?? 'Removed.';
                    const messageText = formatMessage(template, {
                        author: author.username,
                        maxcount: maxPosts.toString(),
                        interval: intervalHours.toString(),
                        subreddit: subreddit.name
                    });

                    if (notifyMethod === 'comment' || notifyMethod === 'both') {
                        const comment = await post.addComment({ text: messageText });
                        await comment.distinguish(true);
                        console.log(`[MHB] Left distinguished removal comment.`);
                    }

                    if (notifyMethod === 'modmail' || notifyMethod === 'both') {
                        await reddit.modMail.createConversation({
                            subredditName: subreddit.name,
                            subject: 'Your post was removed',
                            body: `${messageText}\n\nLink to your post: ${post.permalink}`,
                            to: author.username,
                            isAuthorHidden: true,
                        });
                        console.log(`[MHB] Sent modmail removal notification.`);
                    }
                }

                console.log(`[MHB] Action taken: Removed post ${post.id}.`);
            } else if (actionToTake.includes('report')) {
                const template = await settings.get<string>('removalCommentTemplate') ?? 'Reported.';
                const reasonText = formatMessage(template, {
                    author: author.username,
                    maxcount: maxPosts.toString(),
                    interval: intervalHours.toString(),
                    subreddit: subreddit.name
                });
                await reddit.report(post, { reason: reasonText.substring(0, 100) });
                console.log(`[MHB] Action taken: Reported post ${post.id}.`);
            }

            const banThreshold = await settings.get<number>('banThreshold') ?? 0;
            const banViolationWindowDays = await settings.get<number>('banViolationWindowDays') ?? 30;
            
            if (banThreshold > 0) {
                const violationCount = await recordAndCountViolations(redis, subreddit.id, author.username, banViolationWindowDays);
                console.log(`[MHB] User u/${author.username} has ${violationCount} total violations (Threshold: ${banThreshold}).`);
                
                if (violationCount >= banThreshold) {
                    let banDays = await settings.get<number>('banDurationDays') ?? 1;
                    let permanent = false;
                    if (banDays >= 999) permanent = true;
                    
                    await reddit.banUser({
                        subredditName: subreddit.name,
                        username: author.username,
                        duration: permanent ? undefined : banDays,
                        reason: `Exceeded post frequency limits ${violationCount} times.`,
                        message: `You have been automatically banned for repeatedly violating the post frequency limits.`,
                    });
                    console.log(`[MHB] Action taken: Banned u/${author.username} for ${permanent ? 'permanent' : banDays + ' days'}.`);
                }
            }
        } else {
            // Record this post
            activeRecords.push({ id: post.id, timestamp: now });
            await saveUserPostTimestamps(redis, subreddit.id, author.username, activeRecords, intervalMs);
            console.log(`[MHB] Recorded post ${post.id} for u/${author.username}.`);
        }
    }
});

Devvit.addMenuItem({
    label: 'Grant MHB Hallpass',
    location: 'post',
    forUserType: 'moderator',
    onPress: async (event, context) => {
        const { reddit, redis, ui } = context;
        const post = await reddit.getPostById(event.targetId);
        const targetUsername = post.authorName;
        const subreddit = await reddit.getCurrentSubreddit();
        
        await grantHallpass(redis, subreddit.id, targetUsername);
        console.log(`[MHB] Mod Action: Granted hallpass to u/${targetUsername}.`);
        ui.showToast(`Granted a hallpass to u/${targetUsername}! Their next post will bypass frequency limits.`);
    }
});

Devvit.addMenuItem({
    label: 'Check MHB Status',
    location: 'post',
    forUserType: 'moderator',
    onPress: async (event, context) => {
        const { reddit, redis, ui, settings } = context;
        const post = await reddit.getPostById(event.targetId);
        const targetUsername = post.authorName;
        const subreddit = await reddit.getCurrentSubreddit();
        
        const intervalHours = await settings.get<number>('intervalHours') ?? 72;
        const maxPosts = await settings.get<number>('maxPostsPerInterval') ?? 1;
        const gracePeriodMins = await settings.get<number>('gracePeriodMins') ?? 30;
        const ignoreAutoMod = await settings.get<boolean>('ignoreAutoModeratorRemoved') ?? true;
        const ignoreMod = await settings.get<boolean>('ignoreModeratorRemoved') ?? true;
        
        const intervalMs = intervalHours * 60 * 60 * 1000;
        const cutoffTime = Date.now() - intervalMs;

        const { activeRecords } = await getActiveUserPosts(redis, reddit, subreddit.id, targetUsername, cutoffTime, gracePeriodMins, ignoreAutoMod, ignoreMod);
        const hasPass = await hasHallpass(redis, subreddit.id, targetUsername);
        
        console.log(`[MHB] Mod Action: Checked status for u/${targetUsername} (${activeRecords.length}/${maxPosts}).`);
        ui.showToast(`u/${targetUsername} has ${activeRecords.length}/${maxPosts} active posts in the last ${intervalHours}h. Hallpass: ${hasPass ? 'YES' : 'NO'}`);
    }
});

Devvit.addTrigger({
    event: 'ModMail',
    onEvent: async (event, context) => {
        const { reddit, redis, settings } = context;
        if (!event.messageAuthor || !event.conversationId) return;

        // Ensure we don't reply to moderators or bots
        const authorName = event.messageAuthor.name;
        const subreddit = await reddit.getCurrentSubreddit();
        
        const mods = await subreddit.getModerators({ limit: 100 }).all();
        const isMod = mods.some(mod => mod.username === authorName);
        if (isMod) return;

        // Check user's history
        const history = await getUserPostTimestamps(redis, subreddit.id, authorName);
        
        // If they have no timestamps in our active window, we ask for a link.
        if (history.length === 0) {
            const conversation = await reddit.modMail.getConversation({ conversationId: event.conversationId });
            
            // Only reply to new, unreplied conversations to avoid spam
            if ((conversation.conversation?.numMessages ?? 0) <= 1) {
                console.log(`[MHB] Modmail trigger: Auto-replying to u/${authorName} because they have no recent logged posts.`);
                await reddit.modMail.reply({
                    conversationId: event.conversationId,
                    body: `Hello, thanks for reaching out. We don't see any recent posts from you in our active logs. If you are messaging about a removed post, please reply with a link to the post so a human moderator can assist you.`,
                    isInternal: false,
                });
            } else {
                console.log(`[MHB] Modmail trigger: Skipping auto-reply for u/${authorName} as conversation already has replies.`);
            }
        } else {
             console.log(`[MHB] Modmail trigger: Skipping auto-reply for u/${authorName} because they have recent activity in logs.`);
        }
    }
});

// Modmail Commands
Devvit.addTrigger({
    event: 'ModMail',
    onEvent: async (event, context) => {
        const { reddit, redis, settings } = context;
        if (!event.messageId || !event.conversationId) return;

        try {
            const convoResponse = await reddit.modMail.getConversation({ conversationId: event.conversationId });
            const message = convoResponse.conversation?.messages?.[event.messageId];
            
            if (!message || !message.bodyMarkdown) return;
            
            // Only respond to moderators
            if (!message.author?.isMod) return;

            // Look for `$summary username` or `$summary u/username`
            const match = message.bodyMarkdown.match(/^\$summary\s+(?:u\/)?([\w-]+)/i);
            if (match) {
                const targetUsername = match[1];
                const subreddit = await reddit.getCurrentSubreddit();

                const intervalHours = await settings.get<number>('intervalHours') ?? 72;
                const maxPosts = await settings.get<number>('maxPostsPerInterval') ?? 1;
                const gracePeriodMins = await settings.get<number>('gracePeriodMins') ?? 30;
                const ignoreAutoMod = await settings.get<boolean>('ignoreAutoModeratorRemoved') ?? true;
                const ignoreMod = await settings.get<boolean>('ignoreModeratorRemoved') ?? true;
                
                const intervalMs = intervalHours * 60 * 60 * 1000;
                const cutoffTime = Date.now() - intervalMs;

                const { activeRecords } = await getActiveUserPosts(redis, reddit, subreddit.id, targetUsername, cutoffTime, gracePeriodMins, ignoreAutoMod, ignoreMod);
                const hasPass = await hasHallpass(redis, subreddit.id, targetUsername);
                
                let body = `### MHB Summary for u/${targetUsername}\n\n`;
                body += `**Status:** ${activeRecords.length}/${maxPosts} active posts in the last ${intervalHours}h.\n`;
                body += `**Hallpass:** ${hasPass ? 'YES' : 'NO'}\n\n`;
                
                if (activeRecords.length > 0) {
                    body += `| Post ID | Timestamp (UTC) |\n`;
                    body += `|---|---|\n`;
                    // Active records are oldest first. Let's list newest first or just chronological.
                    for (const record of activeRecords) {
                        // Devvit doesn't easily expose the title here without fetching the post again, 
                        // so we'll just link the ID
                        body += `| [${record.id}](https://redd.it/${record.id}) | ${new Date(record.timestamp).toUTCString()} |\n`;
                    }
                } else {
                    body += `No active posts found in the time window.\n`;
                }

                await reddit.modMail.reply({
                    conversationId: event.conversationId,
                    body: body,
                    isInternal: true
                });
                console.log(`[MHB] Replied to $summary command for u/${targetUsername}`);
            }
        } catch (e) {
            console.error('[MHB] Error processing ModMail command:', e);
        }
    }
});
export default Devvit;
