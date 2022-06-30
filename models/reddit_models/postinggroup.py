class PostingGroup:
    def __init__(self, latest_post_id, author_name=None, subreddit_name=None, posts=None):
        self.latest_post_id = latest_post_id
        self.author_name=author_name
        self.subreddit_name = subreddit_name
        self.posts = posts
