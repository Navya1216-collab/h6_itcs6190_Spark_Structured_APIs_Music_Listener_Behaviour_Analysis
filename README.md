Prerequisites

Before starting, ensure you have the following installed:

Python 3.x

Download Python

Verify:

python --version


PySpark

Install with pip:

pip install pyspark


Apache Spark

Download Spark

Verify installation:

spark-submit --version


Docker & Docker Compose (Optional)

Docker Install

Docker Compose Install

Project Structure

Your project directory should look like this:
MusicListenerAnalysis/
├── data/
│   ├── listening_logs.csv
│   └── songs_metadata.csv
├── output/
│   ├── user_favorite_genres/
│   ├── avg_listen_time_per_song/
│   ├── genre_loyalty_scores/
│   └── night_owl_users/
├── generate_data.py
├── analysis.py
└── README.md

data/: Input datasets (logs + metadata).

output/: Results from each analysis task.

generate_data.py: Script to generate sample input data.

analysis.py: Main Spark analysis script.

README.md: Documentation and instructions.

Running the Analysis

You can run tasks locally or inside Docker.

1. Generate Data
python generate_data.py


This creates two input files under data/:

listening_logs.csv

songs_metadata.csv

2. Run Analysis
python analysis.py

3. Verify Outputs

Results are saved under output/ in CSV format (inside part files).
Flattened copies can be made with:

for d in user_favorite_genres avg_listen_time_per_song genre_loyalty_scores night_owl_users; do
  part=$(ls output/$d/part-*.csv 2>/dev/null | head -n1)
  if [ -n "$part" ]; then
    cp "$part" "output/${d}.csv"
  fi
done

Overview

This project uses Spark Structured APIs to analyze user listening behavior and extract insights about music preferences, listening patterns, and loyalty to genres.

Objectives

By the end of this assignment you should be able to:

Data Loading & Preparation – Work with Spark DataFrames, join datasets, and clean/transform input.

Data Analysis – Apply grouping, filtering, aggregations, and window functions.

Insight Generation – Translate raw logs into actionable insights on user music consumption.

Dataset
Listening Logs (listening_logs.csv)
Column	Type	Description
user_id	String	Unique identifier for each user
song_id	String	Song being played
timestamp	String	Time when the song was played
duration_sec	Int	Duration of listening in seconds
Songs Metadata (songs_metadata.csv)
Column	Type	Description
song_id	String	Unique identifier of the song
title	String	Song title
artist	String	Artist name
genre	String	Genre of the song
mood	String	Mood of the song (happy, sad…)
Analysis Tasks
1. User’s Favourite Genre

Objective: Identify each user’s most listened-to genre.

Steps:

Count plays by (user_id, genre).

Use a window function to rank by play count.

Pick the top genre per user.

Output: output/user_favorite_genres/

2. Average Listen Time per Song

Objective: Compute the average listening duration for each song.

Steps:

Aggregate average duration_sec grouped by song_id.

Join with metadata to add song details.

Output: output/avg_listen_time_per_song/

3. Genre Loyalty Score

Objective: Measure how loyal users are to their top genre.

Formula:

loyalty_score = top_genre_plays / total_plays


Condition: Keep users with loyalty score ≥ 0.8.

Output: output/genre_loyalty_scores/

4. Night Owl Users

Objective: Identify users who frequently listen at night.

Definition: Plays between 00:00 and 04:59.

Rule: A user is a night owl if:

≥ 5 plays during this window OR

Night plays ≥ 20% of total plays.

Output: output/night_owl_users/

Expected Outcomes

user_favorite_genres → Each user’s #1 genre.

avg_listen_time_per_song → Song-level average durations.

genre_loyalty_scores → High-loyalty users (≥ 0.8).

night_owl_users → Users who are active late at night.