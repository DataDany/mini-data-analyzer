from cleaners.data_cleaner import DataCleaner
from constants.columns_name import ColumnsName

def test_data_cleaner_casts_and_hashtags(sample_df_raw):
    df_clean = DataCleaner.clean_tweets_data(sample_df_raw)

    dtypes = dict(df_clean.dtypes)
    assert dtypes["date"] == "date"
    assert dtypes["user_favourites"] == "bigint"
    assert dtypes["user_friends"] == "bigint"
    assert dtypes["user_followers"] == "bigint"
    assert dtypes[ColumnsName.IS_RETWEET] == "boolean"

    row0 = df_clean.orderBy(ColumnsName.USER_NAME).first()
    assert row0[ColumnsName.HASHTAGS] == ["Covid", "Health"]

    flags = [
        r[ColumnsName.IS_RETWEET]
        for r in df_clean.orderBy(ColumnsName.USER_NAME)
                         .select(ColumnsName.IS_RETWEET)
                         .collect()
    ]
    assert flags == [True, False, True]
