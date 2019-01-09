# spark query junk drawer
# note: reference to 'career' stats means career stats since 1996-97
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

sc = SparkContext(appName="parquery")
sql = SQLContext(sc)
data = sql.read.parquet("spark/parquet/**/part*")

## career stats (total)
data.where(data.player_name == "Von Wafer").groupBy("player_name").sum().select("player_name", "sum(pts)").show()

## career stats (avg)
data.groupBy("player_name").agg(round(avg("reb"), 2).alias("avg")).orderBy(desc("avg")).show()

# most _ in a game
## e.g., pts
data.select("player_name", "pts", "reb", "ast", "stl", "blk", "to").orderBy(desc("pts")).limit(10).show()

# most times doing _ in a game
## e.g., 'traditional' triple doubles
data.select("player_name").where(data.reb > 9).where(data.ast > 9).where(data.pts > 9).groupBy(data.player_name).count().orderBy(desc("count")).show()

# dense rank stuff
## e.g., most times top-2 scorer in a game
ranked = data.withColumn("rank", dense_rank().over(Window.partitionBy("game_id").orderBy(desc("pts"))))
ranked.where(ranked.rank < 3).groupBy("player_name").count().select("player_name", "count").orderBy(desc("count")).show()

