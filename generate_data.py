import csv, random, os
from datetime import datetime, timedelta

random.seed(42)
os.makedirs("data", exist_ok=True)

genres = ["Pop", "Rock", "Jazz", "Hip-Hop", "Classical", "EDM", "Country"]
moods  = ["Happy", "Sad", "Energetic", "Chill"]
num_songs = 200

songs = []
for sid in range(1, num_songs + 1):
    songs.append({
        "song_id": f"s{sid:04d}",
        "title": f"Song {sid}",
        "artist": f"Artist {random.randint(1,50)}",
        "genre": random.choice(genres),
        "mood": random.choice(moods)
    })

with open("data/songs_metadata.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["song_id","title","artist","genre","mood"])
    w.writeheader()
    w.writerows(songs)

num_users = 60
rows = []
start = datetime(2025, 3, 1, 0, 0, 0)

for uid in range(1, num_users + 1):
    t = start
    plays_per_user = random.randint(80, 140)
    for _ in range(plays_per_user):
        t += timedelta(minutes=random.randint(5, 240))
        s = random.choice(songs)
        duration = random.randint(30, 420)
        rows.append({
            "user_id": f"u{uid:03d}",
            "song_id": s["song_id"],
            "timestamp": t.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_sec": duration
        })

with open("data/listening_logs.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["user_id","song_id","timestamp","duration_sec"])
    w.writeheader()
    w.writerows(rows)

print(f"Generated {len(rows)} listening logs and {len(songs)} songs.")
