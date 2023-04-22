import boto3
import csv
import json
import tweepy
import uuid

# Define your authentication credentials
consumer_key = 'your-consumer-key'
consumer_secret = 'your-consumer_secret-key'
access_token = 'your-access_token'
access_token_secret = 'your-access_token_secret'

# Authenticate with the Twitter API
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Create the API object
api = tweepy.API(auth)

# Define the hashtag and number of tweets to retrieve
hashtag = '#hello lang:en'
num_tweets = 50

# Use the API to search for tweets with the given hashtag
tweets = tweepy.Cursor(api.search_tweets, q=hashtag).items(num_tweets)

# Define your delivery stream name and region
delivery_stream_name = 'your-delivery-stream'
region = 'ap-south-1'

# Create the Kinesis Firehose client
firehose_client = boto3.client('firehose', region_name='ap-south-1')

# Create a CSV file and write the retrieved tweets to it
with open('tweets.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['ID', 'Timestamp', 'Text', 'Username', 'Hashtags', 'Location', 'Followers', 'Retweets', 'Favorites'])
    for tweet in tweets:
        writer.writerow([tweet.id_str, tweet.created_at, tweet.text, tweet.user.screen_name, tweet.entities['hashtags'], tweet.user.location, tweet.user.followers_count, tweet.retweet_count, tweet.favorite_count])

# Send the CSV file to the Kinesis Data Firehose delivery stream
with open('tweets.csv', 'rb') as f:
    data = f.read()
    response = firehose_client.put_record(
        DeliveryStreamName=delivery_stream_name,
        Record={
            'Data': f"{tweet.created_at},{tweet.text},{tweet.user.screen_name},{tweet.entities['hashtags']},{tweet.user.location},{tweet.user.followers
