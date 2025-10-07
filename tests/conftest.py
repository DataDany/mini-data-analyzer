import pytest
from pyspark.sql import SparkSession

from constants.columns_name import ColumnsName


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("tweets_tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_df_raw(spark):
    rows = [
        {
            ColumnsName.TEXT: "Covid cases rising in the United States!",
            ColumnsName.HASHTAGS: "['Covid', 'Health']",
            "date": "2020-03-15",
            "user_created": "2020-01-01 10:20:30",
            "user_favourites": "10",
            "user_friends": "100",
            "user_followers": "500",
            ColumnsName.IS_RETWEET: "true",
            ColumnsName.USER_LOCATION: "United States",
            ColumnsName.USER_NAME: "alice",
            ColumnsName.SOURCE: "Twitter Web App",
        },
        {
            ColumnsName.TEXT: "Markets fall; Christ the Redeemer closed for visitors.",
            ColumnsName.HASHTAGS: "['Markets','Travel']",
            "date": "2020-03-16",
            "user_created": "2019-12-31 08:00:00",
            "user_favourites": "7",
            "user_friends": "50",
            "user_followers": "250",
            ColumnsName.IS_RETWEET: "False",
            ColumnsName.USER_LOCATION: "Brazil ",
            ColumnsName.USER_NAME: "bob",
            ColumnsName.SOURCE: "Twitter for iPhone",
        },
        {
            ColumnsName.TEXT: "GRAMMYs tonight! Music heals.",
            ColumnsName.HASHTAGS: "['Grammys','Music']",
            "date": "2020-01-26",
            "user_created": "2018-06-01 12:00:00",
            "user_favourites": "0",
            "user_friends": "0",
            "user_followers": "1000",
            ColumnsName.IS_RETWEET: "1",
            ColumnsName.USER_LOCATION: "United States",
            ColumnsName.USER_NAME: "charlie",
            ColumnsName.SOURCE: "Twitter for Android",
        },
    ]
    return spark.createDataFrame(rows)