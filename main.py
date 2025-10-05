from loaders.tweets_loader import TweetsLoader
from pyspark.sql import SparkSession

def main():
    spark = (SparkSession.builder
             .appName("tweets_analyzer")
             .getOrCreate())

    loader = TweetsLoader(spark)
    df = loader.load_all_tweets()

    df.show(truncate=False)
    df.printSchema()

    spark.stop()



if __name__ == "__main__":
    main()


