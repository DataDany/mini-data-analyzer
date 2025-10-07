from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col, split, transform, trim, lower, when
from pyspark.sql.types import LongType, DateType, TimestampType, BooleanType

from constants.columns_name import ColumnsName


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
                   .withColumn(ColumnsName.USER_LOCATION, trim(col(ColumnsName.USER_LOCATION)))
                   .withColumn(ColumnsName.USER_NAME, trim(col(ColumnsName.USER_NAME)))
                   .withColumn(ColumnsName.IS_RETWEET,
                               when(lower(trim(col(ColumnsName.IS_RETWEET))).isin("true", "1"), True)
                               .when(lower(trim(col(ColumnsName.IS_RETWEET))).isin("false", "0"), False)
                               .otherwise(None)
                               .cast(BooleanType())
                               )
                   )
        return cleaned
