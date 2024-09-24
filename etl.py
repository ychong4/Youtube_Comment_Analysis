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

	youtube = googleapiclient.discovery.build(
	api_service_name, api_version, developerKey = DEVELOPER_KEY)

	while True:
	
		request = youtube.commentThreads().list(
			part="snippet, replies",
			videoId = 'D7GDTOSNQSk',
			#"ndAQfTzlVjc",
			pageToken=next_page_token,
            maxResults=100	
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
		#print(comments)

		next_page_token = response.get('nextPageToken')

        #if next_page_token is None:
        #break
		#print(next_page_token)
		#print(comments)
		df = pd.DataFrame(comments)
		df.to_csv("comments.csv")
	#s3://myairflowyoutubebucket/
	print(comments)

	return comments
	
	



run_youtube_etl()












