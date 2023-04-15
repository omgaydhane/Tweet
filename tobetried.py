import boto3
import csv
import json
import tweepy
import uuid

# Define your authentication credentials
consumer_key = '6ZYZTRKthMO6dOnkKW9Ch59H7'
consumer_secret = '9BrFHs0qibGxtI30LPIp6oI6GUr4txS8EktkkJMU1D3GCZBeIm'
access_token = '985487935427436544-ZwhoQgUIRk1swlnEkHeOjd6GAZtZ1a4'
access_token_secret = 'XWg2B9ycb0w7d7rjUkwk5DrvtwBcUXHDyaBrBr58L5DKT'

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
delivery_stream_name = 'twitter-data'
region = 'ap-south-1'

# Create the Kinesis Firehose client
firehose_client = boto3.client('firehose', region_name='ap-south-1')

# Create a CSV file and write the retrieved tweets to it
with open('tweets2.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['ID', 'Timestamp', 'Text', 'Username', 'Hashtags', 'Location', 'Followers', 'Retweets', 'Favorites'])
    for tweet in tweets:
        writer.writerow([tweet.id_str, tweet.created_at, tweet.text, tweet.user.screen_name, tweet.entities['hashtags'], tweet.user.location, tweet.user.followers_count, tweet.retweet_count, tweet.favorite_count])

# Send the CSV file to the Kinesis Data Firehose delivery stream
with open('tweets2.csv', 'rb') as f:
    data = f.read()
    response = firehose_client.put_record(
        DeliveryStreamName=delivery_stream_name,
        Record={
            'Data': f"{tweet.created_at},{tweet.text},{tweet.user.screen_name},{tweet.entities['hashtags']},{tweet.user.location},{tweet.user.followers
