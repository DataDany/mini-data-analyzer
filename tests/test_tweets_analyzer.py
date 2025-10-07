from pyspark.sql.types import StructType, StructField, StringType, LongType

from cleaners.data_cleaner import DataCleaner
from analyzers.tweets_analyzer import (
    calculate_hashtags,
    is_retweet_count,
    calculate_avg_user_followers_per_location,
    source_count,
    top_active_users,
)
from constants.columns_name import ColumnsName

def test_calculate_hashtags(sample_df_raw):
    df_clean = DataCleaner.clean_tweets_data(sample_df_raw)
    res = calculate_hashtags(df_clean)
    tags = {r[ColumnsName.HASHTAGS]: r["count"] for r in res.collect()}
    assert tags.get("covid") == 1
    assert tags.get("health") == 1
    assert tags.get("markets") == 1
    assert tags.get("travel") == 1
    assert tags.get("grammys") == 1
    assert tags.get("music") == 1

def test_is_retweet_count(sample_df_raw):
    df_clean = DataCleaner.clean_tweets_data(sample_df_raw)
    res = is_retweet_count(df_clean).collect()
    counts = {r[ColumnsName.IS_RETWEET]: r["count"] for r in res}
    assert counts[True] == 2
    assert counts[False] == 1



def test_avg_user_followers_per_location_dedup_per_user(sample_df_raw, spark):
    # extra_df tylko z kolumnami, których używa funkcja
    schema = StructType([
        StructField(ColumnsName.USER_NAME, StringType(), True),
        StructField(ColumnsName.USER_FOLLOWERS, LongType(), True),
        StructField(ColumnsName.USER_LOCATION, StringType(), True),
    ])
    extra_df = spark.createDataFrame(
        [("alice", 9999, "United States")],
        schema=schema
    )

    base = sample_df_raw.unionByName(extra_df, allowMissingColumns=True)

    df_clean = DataCleaner.clean_tweets_data(base)
    res = calculate_avg_user_followers_per_location(df_clean)

    rows = {r[ColumnsName.USER_LOCATION]: r["avg_user_followers"] for r in res.collect()}
    assert round(rows["United States"], 1) == 750.0
    assert round(rows["Brazil"], 1) == 250.0


def test_source_count_and_top_active_users(sample_df_raw):
    df_clean = DataCleaner.clean_tweets_data(sample_df_raw)
    sc = source_count(df_clean)
    srcs = {r[ColumnsName.SOURCE]: r["count"] for r in sc.collect()}
    assert srcs["Twitter Web App"] == 1
    assert srcs["Twitter for iPhone"] == 1
    assert srcs["Twitter for Android"] == 1

    top = top_active_users(df_clean, top_n=2).collect()
    assert len(top) == 2
    assert all(r["tweet_count"] == 1 for r in top)
