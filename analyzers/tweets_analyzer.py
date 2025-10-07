from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer, trim, lower, count, lit, avg

from constants.columns_name import ColumnsName


# AGREGATION

def calculate_hashtags(df: DataFrame, top_n: int | None = None) -> DataFrame:
    """
    Counts the frequency of hashtags used in tweets.

    Normalizes hashtags to lowercase, removes null or empty values,
    and sorts them in descending order by occurrence count.
    :param df:
    :param top_n:
    :return: DataFrame
    """
    out = (
        df
        .select(explode_outer(ColumnsName.HASHTAGS).alias(ColumnsName.HASHTAGS))
        .filter(col(ColumnsName.HASHTAGS).isNotNull() & (col(ColumnsName.HASHTAGS) != ""))
        .select(trim(lower(col(ColumnsName.HASHTAGS))).alias(ColumnsName.HASHTAGS))
        .groupBy(ColumnsName.HASHTAGS)
        .agg(count(lit(1)).alias("count"))
        .orderBy(col("count").desc(), col(ColumnsName.HASHTAGS).asc())
    )
    return out.limit(top_n) if top_n else out


def is_retweet_count(df: DataFrame) -> DataFrame:
    return (df.groupBy(ColumnsName.IS_RETWEET)
            .agg(count(lit(1)).alias("count"))
            .orderBy(col("count").desc())
            )


def calculate_avg_user_followers_per_location(df: DataFrame) -> DataFrame:
    """
    Computes the average number of followers per user location.
    :param df:
    :return: DataFrame
    """
    base = (
        df.select(ColumnsName.USER_NAME, ColumnsName.USER_FOLLOWERS, ColumnsName.USER_LOCATION)
        .filter(col(ColumnsName.USER_NAME).isNotNull() & col(ColumnsName.USER_LOCATION).isNotNull())
        .dropDuplicates([ColumnsName.USER_NAME])
    )
    return (
        base.groupBy(ColumnsName.USER_LOCATION)
        .agg(avg(ColumnsName.USER_FOLLOWERS).alias("avg_user_followers"))
        .orderBy(col("avg_user_followers").desc(), col(ColumnsName.USER_LOCATION).asc())
    )


def source_count(df: DataFrame) -> DataFrame:
    """
    Counts the number of tweets coming from each device/source
    :param df:
    :return: DataFrame
    """
    return (df.groupBy(ColumnsName.SOURCE)
            .agg(count(lit(1)).alias("count"))
            .orderBy(col("count").desc())
            )


def source_and_category_count(df: DataFrame) -> DataFrame:
    """
    Counts the number of tweets per source and category combination.
    :param df:
    :return: DataFrame
    """
    return (
        df.groupBy("source", "category")
        .agg(count(lit(1)).alias("count"))
        .orderBy(col("count").desc())
    )


def top_active_users(df: DataFrame, top_n: int = 10) -> DataFrame:
    """
    Counts the number of posts shared by users.
    :param df:
    :param top_n:
    :return: DataFrame
    """
    return (
        df.groupBy("user_name")
        .agg(count(lit(1)).alias("tweet_count"))
        .orderBy(col("tweet_count").desc())
        .limit(top_n)
    )
