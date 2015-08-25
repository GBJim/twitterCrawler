require 'tweetstream'
require 'json'
require 'mongo'
require 'time'

include Mongo

TweetStream.configure do |config|
  config.consumer_key       = 'CPj6F72mHUi1tmH3TcQjL1sWT'
  config.consumer_secret    = 'hSTtahV2HGhZuasPCzueHKplgc2n6CUGBbgGZkcArVBr3yXhso'
  config.oauth_token        = '2383540880-bY8obZ1TzsdGkjEMFYoha9r2rtRj5vkHZ5lF4ub'
  config.oauth_token_secret = 'fa5vK3szB3Tax3zdD4Hm7htXKtDnhsPVcK1PFIrOVq9Aq'
  config.auth_method        = :oauth
end


keyFilter = %w(id text lang media created_at user coordinates place entities)
userFilter = %w(id name screen_name lang location description statuses_count)
#puts keyFilter



while true do
begin

  mongo_client = MongoClient.new("localhost", 27017)
  coll = mongo_client['idea']['geo_tweets_2']

  dataList = []
  TweetStream::Client.new.locations(-180,-90,180,90) do |status|
  # The status object is a special Hash with
  # method access to its keys.
  #da(status.to_h)
    data = status.to_h
    data.select!{|key,value| keyFilter.include? key.to_s}
    data[:user].select!{|key,value| userFilter.include? key.to_s}
    data[:created_at] = Time.parse data[:created_at]
  #puts JSON.pretty_generate(data)
    dataList.push(data)
    #puts data
    if dataList.length > 10000#10000
  	 bulk = coll.initialize_ordered_bulk_op

  	 dataList.each do |data|
  		  bulk.insert(data)
  	 end
  	 p bulk.execute
     puts "Insert"
  	 dataList = []
    end

  
  end
rescue
  puts "Hey"
  sleep 10
end
end
#puts JSON.pretty_generate(x)
#rtc = retweet count id, text, lang, hashtags(emtity), media, created, screenname STC = "statuses_count,

