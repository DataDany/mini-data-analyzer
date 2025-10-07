from enum import Enum


class ColumnsName(str, Enum):
    USER_NAME = "user_name"
    USER_LOCATION = "user_location"
    USER_DESCRIPTION = "user_description"
    USER_CREATED = "user_created"
    USER_FOLLOWERS = "user_followers"
    USER_FRIENDS = "user_friends"
    USER_FAVOURITES = "user_favourites"
    USER_VERIFIED = "user_verified"
    DATE = "date"
    TEXT = "text"
    HASHTAGS = "hashtags"
    SOURCE = "source"
    IS_RETWEET = "is_retweet"
    CATEGORY = "category"
    ID = "id"
    TIMESTAMP = "timestamp"
    SYMBOLS = "symbols"
    COMPANY_NAMES = "company_names"
    URL = "url"
    VERIFIED = "verified"
