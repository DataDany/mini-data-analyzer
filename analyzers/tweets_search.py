from pyspark.sql import DataFrame
from pyspark.sql.functions import array_intersect, split, col, size, lower, array, lit, trim

from constants.columns_name import ColumnsName


def search_by_keyword(df: DataFrame, keyword: str, case_insensitive: bool = True) -> DataFrame:
    """
    Searching for tweets by keyword
    :param df:
    :param keyword:
    :param case_insensitive:
    :return: DataFrame
    """
    if case_insensitive:
        return df.filter(lower(col(ColumnsName.TEXT)).contains(keyword.lower()))
    else:
        return df.filter(col(ColumnsName.TEXT).contains(keyword))


def search_by_keywords(df: DataFrame, key_words: list[str], case_insensitive: bool = True) -> DataFrame:
    """
    Searching by list of keywords
    :param df:
    :param key_words:
    :param case_insensitive:
    :return: DataFrame
    """
    if not key_words:
        return df.limit(0)
    text_col = lower(col(ColumnsName.TEXT)) if case_insensitive else col(ColumnsName.TEXT)
    kws = [w.lower() for w in key_words] if case_insensitive else key_words
    kw_array = array(*[lit(w) for w in kws])
    tokens = split(text_col, r"\W+")
    out = (
        df
        .withColumn("key_words_result", array_intersect(tokens, kw_array))
        .filter(size(col("key_words_result")) > 0)
        .drop("key_words_result")
    )
    return out


def tweets_in_location(df: DataFrame, location: str, case_insensitive: bool = True) -> DataFrame:
    """
    Searching for tweets in location
    :param df:
    :param location:
    :param case_insensitive:
    :return: DataFrame
    """
    if case_insensitive:
        return df.filter(
            lower(trim(col(ColumnsName.USER_LOCATION))) == location.strip().lower()
        )
    else:
        return df.filter(
            trim(col(ColumnsName.USER_LOCATION)) == location.strip()
        )