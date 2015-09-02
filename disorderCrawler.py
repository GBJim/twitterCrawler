from twython import TwythonStreamer
from pymongo import MongoClient
from datetime import datetime

class TweetStreammer(TwythonStreamer):

	def tweet_filter(self,data):

		keys = "id text lang media user coordinates place entities".split()
		user_keys = "id name screen_name lang location description statuses_count".split()
		tweet = {key: value for key, value in data.items() if key in keys }
		tweet["user"] = {key: value for key, value in data["user"].items() if key in user_keys }
		tweet["created_at"] = datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S +0000 %Y')
		return tweet


	def __init__(self, consumer_key, consumer_secret, oauth_token, oauth_token_secret):
		super(TweetStreammer, self).__init__(consumer_key, consumer_secret, oauth_token, oauth_token_secret)
		self.tweets_buffer = []
		self.bulk_size = 10000
		self.collection = MongoClient("localhost", 27017)["disorder"]["tweets"]

	def on_success(self, data):

		if "user" not in data:  #Skip the non-tweet request result
			return

		tweet = self.tweet_filter(data)

		self.tweets_buffer.append(tweet)

		print(tweet["text"])
	
		if len(self.tweets_buffer) >= self.bulk_size:
			self.collection.insert(self.tweets_buffer)
			self.tweets_buffer = []


	def on_error(self, status_code, data):
		print(status_code)



consumer_key       = 'CPj6F72mHUi1tmH3TcQjL1sWT'
consumer_secret    = 'hSTtahV2HGhZuasPCzueHKplgc2n6CUGBbgGZkcArVBr3yXhso'
oauth_token        = '2383540880-bY8obZ1TzsdGkjEMFYoha9r2rtRj5vkHZ5lF4ub'
oauth_token_secret = 'fa5vK3szB3Tax3zdD4Hm7htXKtDnhsPVcK1PFIrOVq9Aq'




geo_stream = TweetStreammer(consumer_key, consumer_secret, oauth_token, oauth_token_secret)

geo_stream.statuses.filter(track = "diagnosed,bpd,disorder")