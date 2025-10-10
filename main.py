from src.youtube_api import (
    get_channel_details, get_video,
    get_video_statistics, get_video_comments, get_video_categories
)
from src.load_gcs import upload_to_gcs
import os

def run_youtube_pipeline(query="data engineering", request=None):
    bucket_name = os.getenv("BUCKET_NAME", "adrineto-ba882-fall25-team-6")

    print(f"🚀 Starting YouTube ETL for query: '{query}'")

    # 1️⃣ Extract Data
    videos_df = get_video(query, max_results=10)
    channels_df = get_channel_details(videos_df["channel_id"].dropna().unique().tolist())
    stats_df = get_video_statistics(videos_df["video_id"].tolist())

    # Just the comments from the most relevant video
    if not videos_df.empty:
        comments_df = get_video_comments(videos_df["video_id"].iloc[0])
    else:
        comments_df = None

    categories_df = get_video_categories(region_code="US")

    # 2️⃣ Load to GCS
    upload_to_gcs(videos_df, bucket_name, "videos", prefix=query)
    upload_to_gcs(channels_df, bucket_name, "channels", prefix=query)
    upload_to_gcs(stats_df, bucket_name, "video_stats", prefix=query)
    if comments_df is not None:
        upload_to_gcs(comments_df, bucket_name, "comments", prefix=query)
    upload_to_gcs(categories_df, bucket_name, "video_categories", prefix=query)

    print("🎉 YouTube ETL pipeline executed successfully!")
    return "Pipeline completed successfully!"
