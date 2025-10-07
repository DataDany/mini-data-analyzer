from pyspark.sql import SparkSession

from cleaners.data_cleaner import DataCleaner
from loaders.tweets_loader import TweetsLoader


def main():
    spark = (SparkSession.builder
             .appName("tweets_analyzer")
             .getOrCreate())

    loader = TweetsLoader(spark)
    df_raw = loader.load_all_tweets()

    ### RAW data
    df_raw.show(truncate=False)
    df_raw.printSchema()

    ### Cleaned data
    df_cleaned = DataCleaner.clean_tweets_data(df_raw)
    df_cleaned.show(truncate=False)
    df_cleaned.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
