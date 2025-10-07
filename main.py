from pyspark.sql import SparkSession

from analyzers.tweets_analyzer import calculate_avg_user_followers_per_location, \
    source_count, top_active_users
from analyzers.tweets_search import tweets_in_location, search_by_keyword, search_by_keywords
from cleaners.data_cleaner import DataCleaner
from constants.columns_name import ColumnsName
from loaders.tweets_loader import TweetsLoader

LOCATION = "United States"
KEYWORD = "covid"
KEYWORDS = ["covid", "Christ"]


def main():
    spark = (SparkSession.builder
             .appName("tweets_analyzer")
             .getOrCreate())

    loader = TweetsLoader(spark)
    df_raw = loader.load_all_tweets().cache()

    ### Cleaned data
    df_cleaned = DataCleaner.clean_tweets_data(df_raw)
    df_cleaned.show(truncate=False)
    df_cleaned.printSchema()

    # SEARCH

    print("\nTWEETS SEARCH BY KEYWORDS AND LOCATION")
    (
        df_cleaned
        .transform(lambda df: search_by_keyword(df, KEYWORD))
        .transform(lambda df: tweets_in_location(df, LOCATION))
        .select(ColumnsName.USER_NAME, ColumnsName.TEXT, ColumnsName.USER_LOCATION)
        .show(truncate=False)
    )

    print(f"\nTWEETS IN LOCATION: {LOCATION}")
    tweets_in_location(df_cleaned, LOCATION, case_insensitive=True).show(20, truncate=False)

    print(f"\nTWEETS WITH KEYWORD: '{KEYWORD}'")
    search_by_keyword(df_cleaned, KEYWORD, case_insensitive=True).show(5, truncate=False)

    print(f"\nTWEETS WITH KEYWORDS: '{KEYWORDS}'")
    search_by_keywords(df_cleaned, KEYWORDS, case_insensitive=True).show(5, truncate=False)

    # ANALYZER

    print("\nAVG FOLLOWERS PER LOCATION")
    calculate_avg_user_followers_per_location(df_cleaned).show(5, truncate=False)

    print("\nSOURCE COUNTS")
    source_count(df_cleaned).show(truncate=False)

    print("\nTOP ACTIVE USERS")
    top_active_users(df_cleaned).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
