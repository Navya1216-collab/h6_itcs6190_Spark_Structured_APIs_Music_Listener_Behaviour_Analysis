from pyspark.sql import SparkSession, functions as F, Window as W
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# =========================
# Config (exact to spec)
# =========================
INPUT_DIR = "data"
OUTPUT_DIR = "output"

MIN_LOYALTY = 0.80          # Task 3: keep users with loyalty >= 0.8
NIGHT_OWL_MIN_HOUR = 0      # Task 4: 00:00 inclusive
NIGHT_OWL_MAX_HOUR = 5      # Task 4: 05:00 exclusive
MIN_NIGHT_PLAYS = 5         # "frequently" rule 1
MIN_NIGHT_PCT   = 0.20      # "frequently" rule 2 (20%)

spark = SparkSession.builder.appName("MusicListenerAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# =========================
# Schemas
# =========================
logs_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("song_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("duration_sec", IntegerType(), False),
])

songs_schema = StructType([
    StructField("song_id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("artist", StringType(), False),
    StructField("genre", StringType(), False),
    StructField("mood", StringType(), False),
])

# =========================
# Load
# =========================
logs = (
    spark.read.option("header", True)
    .schema(logs_schema)
    .csv(f"{INPUT_DIR}/listening_logs.csv")
    .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
)

songs = (
    spark.read.option("header", True)
    .schema(songs_schema)
    .csv(f"{INPUT_DIR}/songs_metadata.csv")
)

plays = logs.join(songs, on="song_id", how="inner")

# =========================
# Task 1: favourite genre (max play_count per user)
# =========================
user_genre_counts = plays.groupBy("user_id", "genre").agg(F.count("*").alias("play_count"))
w = W.partitionBy("user_id").orderBy(F.col("play_count").desc(), F.col("genre").asc())
favorite_genre = (
    user_genre_counts
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
    .drop("rn")
)

(favorite_genre.coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUTPUT_DIR}/user_favorite_genres"))

print("Task 1 rows:", favorite_genre.count())

# =========================
# Task 2: average listen time per song (mean duration_sec)
# =========================
avg_listen = (
    logs.groupBy("song_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .join(songs.select("song_id","title","artist","genre"), "song_id", "left")
    .select("song_id","title","artist","genre", F.round("avg_duration_sec", 2).alias("avg_duration_sec"))
)

(avg_listen.coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUTPUT_DIR}/avg_listen_time_per_song"))

print("Task 2 rows:", avg_listen.count())

# =========================
# Task 3: loyalty >= 0.8 (filter on RAW, output rounded)
# =========================
total_plays_per_user = user_genre_counts.groupBy("user_id").agg(F.sum("play_count").alias("total_plays"))
top_genre_per_user = favorite_genre.withColumnRenamed("genre","top_genre") \
                                   .withColumnRenamed("play_count","top_genre_plays")

loyalty_all = (
    top_genre_per_user.join(total_plays_per_user, "user_id")
    .withColumn("loyalty_score_raw", F.col("top_genre_plays") / F.col("total_plays"))
    .withColumn("loyalty_score", F.round("loyalty_score_raw", 3))
    .select("user_id","top_genre","top_genre_plays","total_plays","loyalty_score_raw","loyalty_score")
    .orderBy(F.desc("loyalty_score_raw"), "user_id")
)

# Filter using RAW score and >= (not >)
loyalty_filtered = loyalty_all.where(F.col("loyalty_score_raw") >= F.lit(MIN_LOYALTY)) \
                              .select("user_id","top_genre","top_genre_plays","total_plays","loyalty_score")

(loyalty_filtered.coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUTPUT_DIR}/genre_loyalty_scores"))

print(f"Task 3 rows (loyalty >= {MIN_LOYALTY}):", loyalty_filtered.count())

# =========================
# Task 4: night owls (00:00–04:59) frequently
# Rule: night_plays >= 5 OR night_pct >= 0.20
# =========================
plays_with_hour = plays.withColumn("hour", F.hour("timestamp"))
night_window = plays_with_hour.where((F.col("hour") >= NIGHT_OWL_MIN_HOUR) & (F.col("hour") < NIGHT_OWL_MAX_HOUR))
night_counts = night_window.groupBy("user_id").agg(F.count("*").alias("night_plays"))
total_counts = plays.groupBy("user_id").agg(F.count("*").alias("total_plays_all"))

night_stats = (
    night_counts.join(total_counts, "user_id", "left")
    .withColumn("night_pct_raw", F.col("night_plays") / F.col("total_plays_all"))
    .withColumn("night_pct", F.round("night_pct_raw", 3))
)

night_owl_users = night_stats.where(
    (F.col("night_plays") >= F.lit(MIN_NIGHT_PLAYS)) | (F.col("night_pct_raw") >= F.lit(MIN_NIGHT_PCT))
).select("user_id","night_plays","total_plays_all", "night_pct")

(night_owl_users.coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUTPUT_DIR}/night_owl_users"))

print("Task 4 rows (night_owls):", night_owl_users.count())

print("All tasks complete. Outputs under:", OUTPUT_DIR)
spark.stop()
