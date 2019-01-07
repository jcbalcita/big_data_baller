from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


if __name__ == "__main__":
    sc = SparkContext(appName="CsvToParquet")
    sqlContext = SQLContext(sc)

    int_cols = [
            "player_id",
            "pts",
            "oreb",
            "dreb",
            "reb",
            "ast",
            "stl",
            "blk",
            "to",
            "ftm",
            "fta",
            "fgm",
            "fg3m",
            "fg3a",
            ]

    schema = StructType([
            StructField("player_id", StringType(), True),
            StructField("min", StringType(), True),
            StructField("pts", StringType(), True),
            StructField("oreb", StringType(), True),
            StructField("dreb", StringType(), True),
            StructField("reb", StringType(), True),
            StructField("ast", StringType(), True),
            StructField("stl", StringType(), True),
            StructField("blk", StringType(), True),
            StructField("to", StringType(), True),
            StructField("ftm", StringType(), True),
            StructField("fta", StringType(), True),
            StructField("fgm", StringType(), True),
            StructField("fga", StringType(), True),
            StructField("fg3m", StringType(), True),
            StructField("fg3a", StringType(), True),
            StructField("player_name", StringType(), True),
            StructField("team_id", StringType(), True),
            StructField("team_abbreviation", StringType(), True),
            StructField("start_position", StringType(), True),
            StructField("plus_minus", StringType(), True),
            StructField("game_id", StringType(), True)])

    rdd = sc.textFile("2018.csv").map(lambda line: line.split(","))
    df = sqlContext.createDataFrame(rdd, schema)

    for col in int_cols:
        df = df.withColumn(col, df[col].cast(IntegerType()))

    df.write.parquet('./input-parquet')

