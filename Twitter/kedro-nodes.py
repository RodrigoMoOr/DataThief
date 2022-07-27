import pandas as pd
from typing import Dict
from functions import get_auth, extract_tweets_from_user_timeline, extract_replies_to_user, extract_tweets_from_search, \
    extract_tweets_from_hashtag


def extract_user_timelines(data: pd.DataFrame, params: Dict) -> pd.DataFrame:
    """
    Kedro Node that extracts all tweets in user timelines for the users in the list (DataFrame) given as input
    Args:
        data: DataFrame with list of users to extract
        params: Dict containing Twitter API keys, tokens and secrets
    Returns:
    DataFrame with tweets from timelines
    """

    print('Starting extraction of User Timelines')

    api = get_auth(consumer_key=params.get('twitter_api_consumer_key'),
                   consumer_secret=params.get('twitter_api_consumer_secret'),
                   access_token=params.get('twitter_api_access_token'),
                   access_token_secret=params.get('twitter_api_access_secret'))

    users_tweets = list()

    for index, row in data.iterrows():
        if pd.notna(row['username_twitter']):
            user_tweets = extract_tweets_from_user_timeline(
                api=api, screen_name=row['username_twitter'], user_type=row['type'])
            if user_tweets is not None and len(user_tweets) > 0:
                users_tweets.extend(user_tweets)

    return pd.DataFrame(data=users_tweets,
                        columns=['extracted_user', 'type', 'id', 'created_at', 'text', 'likes',
                                 'retweets', 'urls', 'medias', 'hashtags', 'user_id', 'user_screen_name',
                                 'user_verified', 'user_followers', 'user_following', 'location'])


def extract_replies_to_users(data: pd.DataFrame, params: Dict) -> pd.DataFrame:
    """
    Extract up to 7 days worth of reply tweets for each user in the list (DataFrame) given as input
    Args:
        data: DataFrame with list of users to extract

    Returns:
    DataFrame with tweets from replies to each user
    """

    print('Starting extraction of Replies to Users')

    api = get_auth(consumer_key=params.get('twitter_api_consumer_key'),
                   consumer_secret=params.get('twitter_api_consumer_secret'),
                   access_token=params.get('twitter_api_access_token'),
                   access_token_secret=params.get('twitter_api_access_secret'))

    replies_to_users = list()

    for index, row in data.iterrows():
        if pd.notna(row['username_twitter']):
            replies_to_user_en = extract_replies_to_user(
                api=api, username=row['username_twitter'], language='en', extraction_size=10000)
            replies_to_user_es = extract_replies_to_user(
                api=api, username=row['username_twitter'], language='es', extraction_size=10000)

            if replies_to_user_en is not None and len(replies_to_user_en) > 0:
                replies_to_users.extend(replies_to_user_en)

            if replies_to_user_es is not None and len(replies_to_user_es) > 0:
                replies_to_users.extend(replies_to_user_es)

    return pd.DataFrame(data=replies_to_users,
                        columns=['extracted_user', 'requested_language', 'id', 'created_at', 'text', 'language',
                                 'likes', 'retweets', 'urls', 'medias', 'hashtags', 'user_id',
                                 'user_screen_name', 'user_verified', 'user_followers', 'user_following', 'location'])


def extract_search_users(data: pd.DataFrame, params: Dict) -> pd.DataFrame:
    """
    Extract up to 7 days worth of tweets when searching for each user in the list (DataFrame) given as input
    Args:
        data: DataFrame with list of users to extract

    Returns:
    DataFrame with tweets from the search for each user
    """

    print('Starting extraction of Search Users')

    api = get_auth(consumer_key=params.get('twitter_api_consumer_key'),
                   consumer_secret=params.get('twitter_api_consumer_secret'),
                   access_token=params.get('twitter_api_access_token'),
                   access_token_secret=params.get('twitter_api_access_secret'))

    tweets_in_search_users = list()

    for index, row in data.iterrows():
        if pd.notna(row['username_twitter']):
            search_user_en = extract_tweets_from_search(
                api=api, query=row['username_twitter'], language='en', extraction_size=10000)
            search_user_es = extract_tweets_from_search(
                api=api, query=row['username_twitter'], language='es', extraction_size=10000)

            if search_user_en is not None and len(search_user_en) > 0:
                tweets_in_search_users.extend(search_user_en)

            if search_user_es is not None and len(search_user_es) > 0:
                tweets_in_search_users.extend(search_user_es)

    return pd.DataFrame(data=tweets_in_search_users,
                        columns=['searched_user', 'requested_language', 'id', 'created_at', 'text', 'language',
                                 'likes', 'retweets', 'urls', 'medias', 'hashtags', 'user_id',
                                 'user_screen_name', 'user_verified', 'user_followers', 'user_following', 'location'])


def extract_tweets_from_hashtags(data: pd.DataFrame, params: Dict) -> pd.DataFrame:
    """
    Extract up to 7 days worth of tweets for each hashtag in the list (DataFrame) given as input
    Args:
        data: DataFrame with list of hashtags to extract

    Returns:
    DataFrame with tweets from searching to each hashtag
    """

    print('Starting extraction of Hashtags')

    api = get_auth(consumer_key=params.get('twitter_api_consumer_key'),
                   consumer_secret=params.get('twitter_api_consumer_secret'),
                   access_token=params.get('twitter_api_access_token'),
                   access_token_secret=params.get('twitter_api_access_secret'))

    hashtags = data
    hashtags['text'] = data['hashtag'].apply(lambda hashtag: hashtag.replace('#', ''))
    tweets_in_hashtags = list()

    for index, row in hashtags.iterrows():
        if pd.notna(row['text']):
            hashtag_tweets_en = extract_tweets_from_hashtag(
                api=api, hashtag=row['text'], language='en', extraction_size=10000)
            hashtag_tweets_es = extract_tweets_from_hashtag(
                api=api, hashtag=row['text'], language='es', extraction_size=10000)

            if hashtag_tweets_en is not None and len(hashtag_tweets_en) > 0:
                tweets_in_hashtags.extend(hashtag_tweets_en)

            if hashtag_tweets_es is not None and len(hashtag_tweets_es) > 0:
                tweets_in_hashtags.extend(hashtag_tweets_es)

    return pd.DataFrame(data=tweets_in_hashtags,
                        columns=['hashtag', 'requested_language', 'id', 'created_at', 'text', 'language',
                                 'likes', 'retweets', 'urls', 'medias', 'hashtags', 'user_id',
                                 'user_screen_name', 'user_verified', 'user_followers', 'user_following', 'location'])


def extract_tweets_from_queries(data: pd.DataFrame, params: Dict) -> pd.DataFrame:
    """
    Extract up to 7 days worth of tweets for each query in the list (DataFrame) given as input
    Args:
        data: DataFrame with list of queries to extract

    Returns:
    DataFrame with tweets from searching to each query
    """

    print('Starting extraction of Search Queries')

    api = get_auth(consumer_key=params.get('twitter_api_consumer_key'),
                   consumer_secret=params.get('twitter_api_consumer_secret'),
                   access_token=params.get('twitter_api_access_token'),
                   access_token_secret=params.get('twitter_api_access_secret'))

    tweets_in_queries = list()

    for index, row in data.iterrows():
        tweets_in_query_en = extract_tweets_from_search(
            api=api, query=row['query'], language='en', extraction_size=10000)
        tweets_in_query_es = extract_tweets_from_search(
            api=api, query=row['query'], language='en', extraction_size=10000)

        if tweets_in_query_en is not None and len(tweets_in_query_en) > 0:
            tweets_in_queries.extend(tweets_in_query_en)

        if tweets_in_query_es is not None and len(tweets_in_query_es) > 0:
            tweets_in_queries.extend(tweets_in_query_es)

    return pd.DataFrame(data=tweets_in_queries,
                        columns=['search_query', 'requested_language', 'id', 'created_at', 'text', 'language',
                                 'likes', 'retweets', 'urls', 'medias', 'hashtags', 'user_id',
                                 'user_screen_name', 'user_verified', 'user_followers', 'user_following', 'location'])
