from cleaners.data_cleaner import DataCleaner
from loaders.tweets_loader import TweetsLoader
from pyspark.sql import SparkSession

def main():
    spark = (SparkSession.builder
             .appName("tweets_analyzer")
             .getOrCreate())

    loader = TweetsLoader(spark)
    df_raw = loader.load_all_tweets()

### RAW data
    df_raw.show(truncate=False)
    df_raw.printSchema()

    df_cleaned = DataCleaner.clean_tweets_data(df_raw)

### Cleaned data
    df_cleaned.show(truncate=False)
    df_cleaned.printSchema()

    spark.stop()



if __name__ == "__main__":
    main()


