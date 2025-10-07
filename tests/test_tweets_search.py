from cleaners.data_cleaner import DataCleaner
from analyzers.tweets_search import search_by_keyword, search_by_keywords, tweets_in_location
from constants.columns_name import ColumnsName

def test_search_by_keyword_case_insensitive(sample_df_raw):
    df_clean = DataCleaner.clean_tweets_data(sample_df_raw)
    out = search_by_keyword(df_clean, "COVID", case_insensitive=True)
    assert out.count() == 1
    assert out.first()[ColumnsName.USER_NAME] == "alice"

    out2 = search_by_keyword(df_clean, "COVID", case_insensitive=False)
    assert out2.count() == 0

def test_search_by_keywords_intersection(sample_df_raw):
    df_clean = DataCleaner.clean_tweets_data(sample_df_raw)
    out = search_by_keywords(df_clean, ["covid", "christ"], case_insensitive=True)
    assert out.count() == 2
    users = {r[ColumnsName.USER_NAME] for r in out.select(ColumnsName.USER_NAME).collect()}
    assert users == {"alice", "bob"}

def test_tweets_in_location_trim_and_case(sample_df_raw):
    df_clean = DataCleaner.clean_tweets_data(sample_df_raw)
    us = tweets_in_location(df_clean, "united states", case_insensitive=True)
    assert us.count() == 2
    br = tweets_in_location(df_clean, "Brazil", case_insensitive=True)
    assert br.count() == 1
    br_cs = tweets_in_location(df_clean, "brazil", case_insensitive=False)
    assert br_cs.count() == 0
