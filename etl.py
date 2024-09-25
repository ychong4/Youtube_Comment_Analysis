import os
import pandas as pd
import json
from datetime import datetime
import s3fs

import googleapiclient.discovery

def run_youtube_etl():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "AIzaSyChRV4Vsu1Srvr1JUCYSBqkglXzo13GGds"
    next_page_token = None

    # Build YouTube API service
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=DEVELOPER_KEY
    )
    
    comments = []
    total_comments = 0
    limit = 1000  # Set the limit to 1000 comments

    # Loop to fetch all comments, handling pagination
    while True:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId='D7GDTOSNQSk',  # YouTube video ID
            pageToken=next_page_token,
            maxResults=100  # Fetch 100 comments per page
        )

        response = request.execute()
        items = response.get('items', [])

        # Extract comments from each item
        for item in items:
            author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
            comment_text = item['snippet']['topLevelComment']['snippet']['textOriginal']
            publish_time = item['snippet']['topLevelComment']['snippet']['publishedAt']
            comment_info = {
                'author': author,
                'comment': comment_text,
                'published_at': publish_time
            }

            comments.append(comment_info)
            total_comments += 1

            # Stop if we have reached the limit of 1000 comments
            if total_comments >= limit:
                break

        # Break if we hit the limit in the middle of a page
        if total_comments >= limit:
            break

        # Get the next page token for pagination
        next_page_token = response.get('nextPageToken')

        # Break the loop if no more pages are left
        if next_page_token is None:
            break

    # Convert the comments list to a DataFrame and save to CSV
    df = pd.DataFrame(comments)
    df.to_csv("comments.csv", index=False)

    print(df)
    return comments


	



run_youtube_etl()












