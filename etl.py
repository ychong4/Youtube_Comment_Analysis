import os
import pandas as pd
import json
from datetime import datetime
import s3fs
import googleapiclient.discovery

import re
import unicodedata
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

import pandas as pd
# Load model directly
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
from pysentimiento import create_analyzer

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

    return comments

def text_cleaning():
    import nltk
    # Download NLTK resources
    nltk.download('stopwords')
    nltk.download('punkt_tab')
    nltk.download('wordnet')
    # Initialize stopwords and lemmatizer
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()

    df = pd.read_csv("comments.csv")

    def clean_text(text):
	    
        if isinstance(text, str):

            # Normalize the text to remove special encodings
            text = unicodedata.normalize('NFKD', text)

            # Convert to lowercase
            text = text.lower()

            # Remove non-ASCII characters
            text = re.sub(r'[^\x00-\x7F]+', '', text)

            # Remove URLs
            text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)

            # Remove HTML tags
            text = re.sub(r'<.*?>', '', text)

            # Remove punctuation
            text = re.sub(r'[^\w\s]', '', text)

            # Remove numbers
            text = re.sub(r'\d+', '', text)

            # Tokenize
            words = word_tokenize(text)

            # Remove stopwords and lemmatize
            words = [lemmatizer.lemmatize(word) for word in words if word not in stop_words]

            # Join words back into a single string
            return ' '.join(words)
        else:
            return text

    # Apply text cleaning function to the DataFrame
    df['cleaned_comment'] = df['comment'].apply(clean_text)

    # Remove rows where 'cleaned_comment' is empty or only whitespace
    df = df[df['cleaned_comment'].str.strip().astype(bool)]

    # Save to csv format
    df.to_csv("cleaned_comments.csv")

def sentiment_analysis():
    pipe = pipeline("text-classification", model="finiteautomata/bertweet-base-sentiment-analysis")

    def predict_sentiment(text):
        if isinstance(text, str) and len(text.strip()) > 0:
            # Use the pipeline to predict sentiment
            result = pipe(text)
            return result[0]['label']
        else:
            return 'NEU'
    
    df = pd.read_csv("cleaned_comments.csv")
    df['sentiment'] = df['cleaned_comment'].apply(predict_sentiment)
    df.to_csv("comments_with_sentiment.csv", index=False)


run_youtube_etl()
text_cleaning()
sentiment_analysis()













