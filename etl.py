import os
import pandas as pd
import json
from datetime import datetime
import s3fs

import googleapiclient.discovery


# Disable OAuthlib's HTTPS verification when running locally.
# *DO NOT* leave this option enabled in production.
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "AIzaSyChRV4Vsu1Srvr1JUCYSBqkglXzo13GGds"

youtube = googleapiclient.discovery.build(
api_service_name, api_version, developerKey = DEVELOPER_KEY)

#request = youtube.commentThreads().list(
#    part="snippet, replies",
#    videoId="q8q3OFFfY6c"
#)
request = youtube.commentThreads().list(
    part="snippet, replies",
    #parentId="UgzDE2tasfmrYLyNkGt4AaABAg"
    videoId = "ndAQfTzlVjc"
)

response = request.execute()

#print(response)

comments = []
def process_comments(response):
	items = response.get('items', [])
	
	for item in items:
			author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
			comment_text = item['snippet']['topLevelComment']['snippet']['textOriginal']
			publish_time = item['snippet']['topLevelComment']['snippet']['publishedAt']
			comment_info = {'author': author, 
						'comment': comment_text, 'published_at': publish_time}

			comments.append(comment_info)
	print(f'Finished processing {len(comments)} comments.')
	return comments

process_comments(response)
print(comments)

df = pd.DataFrame(comments)
df.to_csv("comments.csv")







