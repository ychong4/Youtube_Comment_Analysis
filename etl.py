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
	DEVELOPER_KEY = "--Enter Your Developer Key Here--"

	youtube = googleapiclient.discovery.build(
	api_service_name, api_version, developerKey = DEVELOPER_KEY)

	request = youtube.commentThreads().list(
		part="snippet, replies",
		videoId = "ndAQfTzlVjc"
	)

	response = request.execute()
	comments = []
	items = response.get('items', [])
	
	for item in items:
			author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
			comment_text = item['snippet']['topLevelComment']['snippet']['textOriginal']
			publish_time = item['snippet']['topLevelComment']['snippet']['publishedAt']
			comment_info = {'author': author, 
						'comment': comment_text, 'published_at': publish_time}

			comments.append(comment_info)
	df = pd.DataFrame(comments)
	df.to_csv("s3://myairflowyoutubebucket/comments.csv")
















AIzaSyChRV4Vsu1Srvr1JUCYSBqkglXzo13GGds