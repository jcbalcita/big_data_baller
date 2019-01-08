# spark query junk drawer
# note: reference to 'career' stats means career stats since 1996-97

from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as f

sc = SparkContext(appName="parquery")
sql = SQLContext(sc)
data = sql.read.parquet("python/parquet/**/part*")

## career stats (aggregated)
data.where(data.player_name == "Von Wafer").groupBy("player_name").sum().select("player_name", "sum(pts)").show()

# most _ in a game
## e.g., pts
data.select("player_name", "pts", "reb", "ast", "stl", "blk", "to").orderBy(f.desc("pts")).limit(10).show()

# most time doing _ in a game
## e.g., 'traditional' triple doubles
data.select("player_name").where(data.reb > 9).where(data.ast > 9).where(data.pts > 9).groupBy(data.player_name).count().orderBy(f.desc("count")).show()

