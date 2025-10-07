from functools import reduce
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, trim, col

COVID: str = "covid"
FINANCIAL: str = "financial"
GRAMMYS: str = "grammys"


class TweetsLoader:
    def __init__(
            self,
            spark: SparkSession,
            base_path: str = "data_to_load",
            infer_schema: bool = False
    ) -> None:
        self.spark = spark
        self.base_path = base_path
        self.read_options = {
            "header": "true",
            "inferSchema": "true" if infer_schema else "false",
            "multiline": "true",
            "quote": '"',
            "escape": '"'
        }

    def load_all_tweets(self):
        covid_df = self.load_covid_tweets()
        financial_df = self.load_financial_tweets()
        grammy_df = self.load_grammy_tweets()

        dfs: List[DataFrame] = [covid_df, financial_df, grammy_df]
        unioned = self._safe_union_all(dfs)

        return unioned

    def load_covid_tweets(self) -> DataFrame:
        return self._load_single_csv(
            filename="/covid19_tweets.csv",
            category=COVID,
            load_from="covid19_tweets"
        )

    def load_financial_tweets(self) -> DataFrame:
        return self._load_single_csv(
            filename="/financial.csv",
            category=FINANCIAL,
            load_from="financial"
        )

    def load_grammy_tweets(self) -> DataFrame:
        return self._load_single_csv(
            filename="/GRAMMYS_tweets.csv",
            category=GRAMMYS,
            load_from="grammys_tweets"
        )

    def _read_csv(self, path: str) -> DataFrame:
        reader = self.spark.read
        for k, v in self.read_options.items():
            reader = reader.option(k, v)
        return reader.csv(path)

    def _load_single_csv(self, filename: str, category: str, load_from: str) -> DataFrame:
        path = f"{self.base_path}/{filename}"

        df = (
            self._read_csv(path)
            .withColumn("category", lit(category))
            .withColumn("file", lit(load_from))
        )

        if "text" in df.columns:
            df = df.withColumn("text", trim(col("text"))).na.drop()
        else:
            pass

        rec_cnt = df.count()
        print(f"Loaded: {rec_cnt} records from {load_from} ({filename})")

        return df

    @staticmethod
    def _safe_union_all(dfs: List[DataFrame]) -> DataFrame:
        non_empty = [df for df in dfs if df is not None]
        if not non_empty:
            raise ValueError("No DataFrames to union")
        return reduce(
            lambda left, right: left.unionByName(right, allowMissingColumns=True),
            non_empty
        )
