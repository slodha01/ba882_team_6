"""
Transform raw data into staging tables for YouTube data.
"""
import functions_framework
from google.cloud import bigquery
from flask import jsonify

project_id = 'adrineto-qst882-fall25'
dataset_id = 'youtube_staging'
location = 'us-central1'

@functions_framework.http
def task(request):
    client = bigquery.Client(project=project_id, location=location)

    # Define staging table schemas
    table_schemas = {
        "dim_videos": [
            bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("channel_id", "STRING"),
            bigquery.SchemaField("published_at", "TIMESTAMP"),
            bigquery.SchemaField("last_updated", "TIMESTAMP")
        ],
        "dim_channels": [
            bigquery.SchemaField("channel_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("channel_title", "STRING"),
            bigquery.SchemaField("channel_description", "STRING"),
            bigquery.SchemaField("last_updated", "TIMESTAMP")
        ],
        "dim_authors": [
            bigquery.SchemaField("author_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("author_display_name", "STRING"),
            bigquery.SchemaField("last_updated", "TIMESTAMP")
        ],
        "fact_video_statistics": [
            bigquery.SchemaField("video_id", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("view_count", "INTEGER"),
            bigquery.SchemaField("like_count", "INTEGER"),
            bigquery.SchemaField("comment_count", "INTEGER")
        ],
        "fact_comments": [
            bigquery.SchemaField("comment_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("video_id", "STRING"),
            bigquery.SchemaField("author_id", "STRING"),
            bigquery.SchemaField("comment_text", "STRING"),
            bigquery.SchemaField("published_at", "TIMESTAMP")
        ],
    }

    # --- Step 1: Ensure all tables exist ---
    for table_name, schema in table_schemas.items():
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        try:
            client.get_table(table_id)
            print(f"Table already exists: {table_id}")
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            print(f"Created table: {table_id}")

    # --- Step 2: Run transformations ---
    queries = [
        # dim_videos
        """
        MERGE `adrineto-qst882-fall25.youtube_staging.dim_videos` AS T
        USING (
          SELECT DISTINCT
            video_id,
            title,
            description,
            channel_id,
            published_at
          FROM `adrineto-qst882-fall25.youtube_raw.videos`
        ) AS S
        ON T.video_id = S.video_id
        WHEN MATCHED THEN
          UPDATE SET
            T.title = S.title,
            T.description = S.description,
            T.channel_id = S.channel_id,
            T.published_at = S.published_at,
            T.last_updated = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (video_id, title, description, channel_id, published_at, last_updated)
          VALUES (S.video_id, S.title, S.description, S.channel_id, S.published_at, CURRENT_TIMESTAMP());
        """,
        # dim_channels
        """
        MERGE `adrineto-qst882-fall25.youtube_staging.dim_channels` AS T
        USING (
          SELECT DISTINCT
            channel_id,
            channel_title,
            channel_description
          FROM `adrineto-qst882-fall25.youtube_raw.channels`
        ) AS S
        ON T.channel_id = S.channel_id
        WHEN MATCHED THEN
          UPDATE SET
            T.channel_title = S.channel_title,
            T.channel_description = S.channel_description,
            T.last_updated = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (channel_id, channel_title, channel_description, last_updated)
          VALUES (S.channel_id, S.channel_title, S.channel_description, CURRENT_TIMESTAMP());
        """,
        # dim_authors
        """
        MERGE `adrineto-qst882-fall25.youtube_staging.dim_authors` AS T
        USING (
          SELECT DISTINCT
            author_id,
            author_display_name
          FROM `adrineto-qst882-fall25.youtube_raw.comment_authors`
        ) AS S
        ON T.author_id = S.author_id
        WHEN MATCHED THEN
          UPDATE SET
            T.author_display_name = S.author_display_name,
            T.last_updated = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (author_id, author_display_name, last_updated)
          VALUES (S.author_id, S.author_display_name, CURRENT_TIMESTAMP());
        """,
        # fact_video_statistics
        """
        INSERT INTO `adrineto-qst882-fall25.youtube_staging.fact_video_statistics`
        (video_id, date, view_count, like_count, comment_count)
        SELECT
          video_id,
          CURRENT_DATE() AS date,
          view_count,
          like_count,
          comment_count
        FROM `adrineto-qst882-fall25.youtube_raw.video_statistics`;
        """,
        # fact_comments
        """
        MERGE `adrineto-qst882-fall25.youtube_staging.fact_comments` AS T
        USING (
          SELECT
            comment_id,
            video_id,
            author_id,
            text_display AS comment_text,
            published_at
          FROM `adrineto-qst882-fall25.youtube_raw.comments`
        ) AS S
        ON T.comment_id = S.comment_id
        WHEN NOT MATCHED THEN
          INSERT (comment_id, video_id, author_id, comment_text, published_at)
          VALUES (S.comment_id, S.video_id, S.author_id, S.comment_text, S.published_at);
        """
    ]

    results = []
    for i, query in enumerate(queries):
        job = client.query(query)
        job.result()
        results.append(f"Query {i+1} executed successfully")

    return jsonify({
        "status": "success",
        "message": "Incremental transformations completed successfully",
        "results": results
    })
