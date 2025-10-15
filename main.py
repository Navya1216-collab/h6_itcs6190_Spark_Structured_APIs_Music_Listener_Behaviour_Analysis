# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, row_number
from pyspark.sql.window import Window

# -------------------------------
# Start Spark Session
# -------------------------------
spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# -------------------------------
# Load datasets
# -------------------------------
logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Join logs with songs metadata
df = logs.join(songs, on="song_id", how="inner")

# -------------------------------
# Task 1: User Favorite Genres
# -------------------------------
user_genre_count = df.groupBy("user_id", "genre").count()

window_spec = Window.partitionBy("user_id").orderBy(desc("count"))
user_fav_genre = user_genre_count.withColumn(
    "rank", row_number().over(window_spec)
).filter(col("rank") == 1).drop("rank")

print("\n===== Task 1: User Favorite Genres =====")
user_fav_genre.show(truncate=False)

user_fav_genre.write.mode("overwrite").csv("output/user_favorite_genres")

# -------------------------------
# Task 2: Average Listen Time
# -------------------------------
avg_listen_time = df.groupBy("song_id", "title", "artist") \
    .agg(avg("duration_sec").alias("avg_duration_sec"))

print("\n===== Task 2: Average Listen Time per Song =====")
avg_listen_time.show(truncate=False)

avg_listen_time.write.mode("overwrite").csv("output/avg_listen_time_per_song")

# -------------------------------
# Task 3: Genre Loyalty Scores
# -------------------------------
# Total plays per user
user_total = df.groupBy("user_id").count().withColumnRenamed("count", "total_plays")

# Favorite genre plays per user
user_fav = user_fav_genre.select("user_id", "genre", col("count").alias("fav_genre_count"))

# Loyalty score = fav_genre_count / total_plays
loyalty = user_fav.join(user_total, on="user_id") \
    .withColumn("loyalty_score", col("fav_genre_count") / col("total_plays")) \
    .filter(col("loyalty_score") > 0.5)

print("\n===== Task 3: Genre Loyalty Scores (>0.5) =====")
loyalty.show(truncate=False)

loyalty.write.mode("overwrite").csv("output/genre_loyalty_scores")

# -------------------------------
# Task 4: Night Owl Users (12 AM - 5 AM)
# -------------------------------
df = df.withColumn("hour", col("timestamp").substr(12, 2).cast("int"))
night_owls = df.filter((col("hour") >= 0) & (col("hour") < 5)) \
    .select("user_id").distinct()

print("\n===== Task 4: Night Owl Users =====")
night_owls.show(truncate=False)

night_owls.write.mode("overwrite").csv("output/night_owl_users")

# -------------------------------
# Stop Spark
# -------------------------------
spark.stop()