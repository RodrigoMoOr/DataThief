from datetime import datetime
from typing import List
import tweepy
from tweepy.errors import TweepyException


def get_auth(consumer_key: str, consumer_secret: str, access_token: str, access_token_secret) -> tweepy.API:
    """
    Creates a Tweepy Auth object given the credentials
    Args:
        consumer_key: Twitter API Consumer Key
        consumer_secret: Twitter API Consumer Secret Key
        access_token: Twitter API Access Token
        access_token_secret: Twitter API Secret Access Token

    Returns:
    tweepy.API: Tweepy API object
    """

    try:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        print('Successfully authenticated with the Twitter API')

        return tweepy.API(auth, wait_on_rate_limit=True)

    except TweepyException as e:
        print('Error authenticating with Twitter API')
        print(e)


def extract_tweets_from_user_timeline(api: tweepy.API, screen_name: str, user_type: str, count: int = 200,
                                      tweet_mode: str = 'extended', exclude_replies: bool = False,
                                      include_rts: bool = False, extraction_size: int = 10000) -> List:
    """
    Extracts tweets from user timeline

    Args:
        api:Tweepy API
        screen_name: of the user to be extracted
        user_type: applaudo, midas, customer, competitor, or mkt
        count: total amount of tweets per page in the extraction, MAX 200
        tweet_mode: compat or extended
        exclude_replies: true to exclude replies to others, false to include replies to others
        include_rts: retweets made by user, true includes, false does not
        extraction_size: total tweets to extract -> n/count e.g. 10000/200 = 50 pages of extractions

    Returns:
    Arrays of tweets
    """

    print(f'Extracting username: {screen_name}')

    try:
        tweets = tweepy.Cursor(api.user_timeline, screen_name=screen_name, count=count, tweet_mode=tweet_mode,
                               exclude_replies=exclude_replies, include_rts=include_rts).items(extraction_size)

        return [(screen_name, user_type, tweet.id_str, tweet.created_at, tweet.full_text,
                 tweet.favorite_count, tweet.retweet_count,
                 tweet.entities.get('urls', []), tweet.entities.get('media', []), tweet.entities.get('hashtags', []),
                 tweet.user.id_str, tweet.user.screen_name, tweet.user.verified, tweet.user.followers_count,
                 tweet.user.friends_count, tweet.user.location + '')
                for tweet in tweets]

    except TweepyException as e:
        print('Error extracting tweets from user timeline')
        print(e)
        pass


def extract_replies_to_user(api: tweepy.API, username: str, count: int = 200, tweet_mode: str = 'extended',
                            language: str = "en", result_type: str = 'mixed',
                            until: str = datetime.now().strftime("%Y-%m-%d"), extraction_size: int = 200):
    """
    Extract replies to a given user - up to the last 7 days worth of replies
    Args:
        api: tweepy API
        username: of user to extract replies
        count: total amount of tweets per page in the extraction, MAX 200
        tweet_mode: compat or extended
        language: (e.g. en or es)
        result_type: mixed, recent or popular
        until: max date for extraction, byt default date.now()
        extraction_size: total tweets to extract -> n/count e.g. 10000/200 = 50 pages of extractions

    Returns:
    Array of tweets
    """

    print(f'Extracting replies to username: {username}')

    try:
        tweets = tweepy.Cursor(api.search_tweets, q=f"to:{username}", lang=language, tweet_mode=tweet_mode,
                               result_type=result_type, count=count, until=until).items(extraction_size)

        return [(username, language, tweet.id_str, tweet.created_at, tweet.full_text, tweet.user.lang,
                 tweet.favorite_count, tweet.retweet_count,
                 tweet.entities.get('urls', []), tweet.entities.get('media', []), tweet.entities.get('hashtags', []),
                 tweet.user.id_str, tweet.user.screen_name, tweet.user.verified, tweet.user.followers_count,
                 tweet.user.friends_count, tweet.user.location + '')
                for tweet in tweets]

    except TweepyException as e:
        print('Error extracting search query tweets')
        print(e)
        pass


def extract_tweets_from_search(api: tweepy.API, query: str, count: int = 200, tweet_mode: str = 'extended',
                               language: str = "en", result_type: str = 'mixed',
                               until: str = datetime.now().strftime("%Y-%m-%d"), extraction_size=200):
    """
    Extracts up to 7 days worth of tweets in the results of the search query
    Args:
        api: tweepy API
        query: search query to look up and extract
        count: total amount of tweets per page in the extraction, MAX 200
        tweet_mode: compat or extended
        language: (e.g. en or es)
        result_type: mixed, recent or popular
        until: max date for extraction, byt default date.now()
        extraction_size: total tweets to extract -> n/count e.g. 10000/200 = 50 pages of extractions

    Returns:
    Array of tweets
    """

    print(f'Extracting search for: {query}')

    try:
        tweets = tweepy.Cursor(api.search_tweets, q=query, lang=language, tweet_mode=tweet_mode,
                               result_type=result_type, count=count, until=until).items(extraction_size)

        return [(query, language, tweet.id_str, tweet.created_at, tweet.full_text, tweet.user.lang,
                 tweet.favorite_count, tweet.retweet_count,
                 tweet.entities.get('urls', []), tweet.entities.get('media', []), tweet.entities.get('hashtags', []),
                 tweet.user.id_str, tweet.user.screen_name, tweet.user.verified, tweet.user.followers_count,
                 tweet.user.friends_count, tweet.user.location + '')
                for tweet in tweets]

    except TweepyException as e:
        print('Error extracting search query tweets')
        print(e)


def extract_tweets_from_hashtag(api: tweepy.API, hashtag: str, count: int = 200, tweet_mode: str = 'extended',
                                language: str = "en", result_type: str = 'mixed',
                                until=datetime.now().strftime("%Y-%m-%d"), extraction_size=200):
    """
    Extract up to 7 days worth of tweets in a hashtag
    Args:
        api: tweepy API
        hashtag: as plain text (query) to extract
        count: total amount of tweets per page in the extraction, MAX 200
        tweet_mode: compat or extended
        language: (e.g. en or es)
        result_type: mixed, recent or popular
        until: max date for extraction, byt default date.now()
        extraction_size: total tweets to extract -> n/count e.g. 10000/200 = 50 pages of extractions

    Returns:
    Array of tweets
    """

    print(f'Extracting hashtag: {hashtag}')

    try:
        tweets = tweepy.Cursor(api.search_tweets, q=f"#{hashtag}", lang=language, tweet_mode=tweet_mode,
                               result_type=result_type, count=count, until=until).items(extraction_size)

        return [(f"#{hashtag}", language, tweet.id_str, tweet.created_at, tweet.full_text,
                 tweet.user.lang, tweet.favorite_count, tweet.retweet_count,
                 tweet.entities.get('urls', []), tweet.entities.get('media', []), tweet.entities.get('hashtags', []),
                 tweet.user.id_str, tweet.user.screen_name, tweet.user.verified, tweet.user.followers_count,
                 tweet.user.friends_count, tweet.user.location + '')
                for tweet in tweets]

    except TweepyException as e:
        print('Error extracting tweets from hashtag')
        print(e)
        pass
