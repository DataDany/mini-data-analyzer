from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col, split, transform, trim
from pyspark.sql.types import LongType, DateType, TimestampType


class DataCleaner:

    @staticmethod
    def clean_tweets_data(df: DataFrame) -> DataFrame:
        cleaned = (df.withColumn("hashtags",
                                 transform(
                                     split(regexp_replace(col("hashtags"), r"[ \[\]']", ""),
                                           r"\s*,\s*"),
                                     lambda h: trim(h))
                                 )
                   .withColumn("date", col("date").cast(DateType()))
                   .withColumn("user_created", col("user_created").cast(TimestampType()))
                   .withColumn("user_favourites", col("user_favourites").cast(LongType()))
                   .withColumn("user_friends", col("user_friends").cast(LongType()))
                   .withColumn("user_followers", col("user_followers").cast(LongType()))
                   )
        return cleaned

