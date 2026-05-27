import { SettingsFormField, Devvit } from '@devvit/public-api';

export const appSettings: SettingsFormField[] = [
    {
        type: 'number',
        name: 'maxPostsPerInterval',
        label: 'Maximum posts allowed per interval',
        defaultValue: 1,
    },
    {
        type: 'number',
        name: 'intervalHours',
        label: 'Time interval (in hours)',
        defaultValue: 72,
    },
    {
        type: 'number',
        name: 'gracePeriodMins',
        label: 'Grace period (in minutes)',
        helpText: 'If a user deletes their post and reposts within this window, it will not count against their quota.',
        defaultValue: 30,
    },
    {
        type: 'select',
        name: 'actionToTake',
        label: 'Action to take on violation',
        options: [
            { label: 'Remove Post', value: 'remove' },
            { label: 'Report Post', value: 'report' }
        ],
        defaultValue: ['remove'],
        multiSelect: false
    },
    {
        type: 'string',
        name: 'removalReasonConfigInstruction',
        label: 'How to Configure Removal Reason:',
        helpText: 'To select which subreddit rule/reason is attached to removed posts, do NOT use this settings menu. Instead, go to the subreddit front page three dot menu and click "MHB: Configure Removal Reason".',
        defaultValue: 'Read help text above.',
    },
    {
        type: 'select',
        name: 'removalNotificationMethod',
        label: 'Notify user of removal via:',
        options: [
            { label: 'Comment (Stickied)', value: 'comment' },
            { label: 'Modmail', value: 'modmail' },
            { label: 'Both', value: 'both' },
            { label: 'None', value: 'none' }
        ],
        defaultValue: ['comment'],
        multiSelect: false
    },
    {
        type: 'string',
        name: 'removalCommentTemplate',
        label: 'Removal Message Template (for Comment/Modmail)',
        helpText: 'Use {author}, {maxcount}, {interval}, and {subreddit} as tags.',
        defaultValue: 'Hello {author}, your post was removed because you have exceeded the limit of {maxcount} posts per {interval} hours in r/{subreddit}.',
    },
    {
        type: 'group',
        label: 'Auto-Ban Configuration',
        fields: [
            {
                type: 'number',
                name: 'banThreshold',
                label: 'Ban Threshold',
                helpText: 'Number of violations before issuing a ban. Set to 0 to disable autoban.',
                defaultValue: 0,
            },
            {
                type: 'number',
                name: 'banViolationWindowDays',
                label: 'Violation Window (Days)',
                helpText: 'Number of days a violation stays on a user\'s record. Example: "3" violations in "30" days.',
                defaultValue: 30,
            },
            {
                type: 'number',
                name: 'banDurationDays',
                label: 'Ban Duration (Days)',
                helpText: 'Set to 999 for permanent ban.',
                defaultValue: 1,
            }
        ]
    },
    {
        type: 'group',
        label: 'Exemptions',
        fields: [
            {
                type: 'boolean',
                name: 'exemptModeratorPosts',
                label: 'Exempt Moderator Posts',
                defaultValue: true,
            },
            {
                type: 'boolean',
                name: 'exemptTextPosts',
                label: 'Exempt Text (Self) Posts',
                defaultValue: false,
            },
            {
                type: 'boolean',
                name: 'exemptLinkPosts',
                label: 'Exempt Link Posts',
                defaultValue: false,
            },
            {
                type: 'string',
                name: 'exemptFlairKeyword',
                label: 'Exempt Author Flair Keyword',
                helpText: 'Authors with this keyword in their flair will be exempt.',
                defaultValue: '',
            },
            {
                type: 'string',
                name: 'exemptTitleKeyword',
                label: 'Exempt Title Keyword',
                helpText: 'Posts with this keyword in their title will be exempt.',
                defaultValue: '',
            }
        ]
    },
    {
        type: 'boolean',
        name: 'lockThread',
        label: 'Lock offending posts',
        helpText: 'If enabled, posts removed by MHB will be automatically locked, preventing further comments.',
        defaultValue: true,
    },
    {
        type: 'boolean',
        name: 'ignoreAutoModeratorRemoved',
        label: 'Ignore AutoModerator-removed posts',
        helpText: 'If enabled, posts removed by AutoModerator will NOT count against a user\'s rate limit. If disabled, they WILL count.',
        defaultValue: true,
    },
    {
        type: 'boolean',
        name: 'ignoreModeratorRemoved',
        label: 'Ignore Moderator-removed posts',
        helpText: 'If enabled, posts removed by human moderators will NOT count against a user\'s rate limit. If disabled, they WILL count.',
        defaultValue: true,
    }
];
