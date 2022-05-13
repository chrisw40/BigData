from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from textblob import TextBlob
from elasticsearch import Elasticsearch
from datetime import datetime as dt
from geopy.geocoders import Nominatim
import re

TCP_IP = 'localhost'
TCP_PORT = 52000



def processTweet(tweet):

        
        tweetData = tweet.split("::")
        INDEX_NAME = "tweet-sentiment"
        ES_HOST = {"host": "localhost", "port": 9200}
        # ElasticSearch: by default we connect to localhost:9200
        es = Elasticsearch(hosts=[ES_HOST], http_auth="elastic:elastic")
        
        # tweetData has to not be empty
        # tweetData[0] = rawLocation and it has to have a comma separating the street, city, state and country. Verifying it's an address
        # The elements inside tweetData[0] have to be some string, other than empty string
        if len(tweetData) > 1 and "," in tweetData[0] and len(tweetData[0].split(',')) > 1:
                
                content = tweetData[1]
                rawLocation = tweetData[0]
                lat = None # initalizing lat to be none, just in case geocode fails
                lon = None # initalizing lon to be none, just in case geocode fails
                state = None # initalizing state to be none, just in case geocode fails
                country = None #  initalizing country to be none, just in case geocode fails
                sentiment = None #  initalizing sentiment to be none, just in case geocode fails
                # (i) Apply Sentiment analysis in "text"
                # Getting Sentiment from text
                if float(TextBlob(content).sentiment.polarity) > 0.1 and float(TextBlob(content).sentiment.polarity) <= 0.3:
                        sentiment = "Semi-Positive"
                elif float(TextBlob(content).sentiment.polarity) > 0.3:
                        sentiment = "Positive"
                elif float(TextBlob(content).sentiment.polarity) < -0.1 and float(TextBlob(content).sentiment.polarity) >= -0.3:
                        sentiment = "Semi-Negative"
                elif float(TextBlob(content).sentiment.polarity) < -0.3:
                        sentiment = "Negative"
                else:
                        sentiment = "Neutral"

                # (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation
                # Getting the geolocation using Nominatim OSM API
                geolocator = Nominatim(user_agent = "twitterSentimentApp")
                geoLocation = geolocator.geocode(rawLocation, addressdetails = True)
                # Make sure geoLocation doesn't equal 'None'
                if geoLocation:
                        lat = geoLocation.raw['lat']
                        lon = geoLocation.raw['lon']
                        country = geoLocation.raw['address']['country']
                        country_code = geoLocation.raw['address']['country_code'].upper() ## Country Code needs to be uppercase in order to do a chloropleth map layer in kibana
                        us = "United States"
                        if us in country:
                                state = geoLocation.raw['address']['state']
                        else:
                                state = "N/A"
                        # Only print tweet info and geo info if geoLocation is True
                        print("\n========================\n")
                        print("Tweet and Sentiment Data: \n")
                        print("tweet: ", content)
                        print("Sentiment: ", sentiment)
                        print("\nGeolocation Data:")
                        print("lat: ", lat)
                        print("lon: ", lon)
                        print("state: ", state)
                        print("country: ", country)
                        print("country code: ", country_code)
                        print("\n========================\n")

                # (iii) Posting the Sentiment, Content, and Geographic Information to ElasticSearch
                if lat != None and lon != None and state != None and country != None and sentiment != None:
                        bulk_data = []
                        data_dict = {}
                        # Adding geographic and tweet info into data dictionary
                        data_dict["geo"] = {"coordinates": {"lat": float(lat), "lon": float(lon)}}
                        data_dict["content"] = content
                        data_dict["sentiment"] = sentiment
                        data_dict["state"] = state
                        data_dict["country"] = country
                        data_dict["country_code"] = country_code
                        data_dict["date"] = dt.now().strftime("%m-%d-%Y %H:%M:%S")
                        op_dict = {
                                "index": {
                                "_index": INDEX_NAME,
                                }
                        }
                        bulk_data.append(op_dict)
                        bulk_data.append(data_dict)
                        # Sending index to Kibana
                        new_index = es.bulk(index=INDEX_NAME, body=bulk_data, refresh=True)
                        print("New index sent to Kibana: '%s'" % new_index)
                        print("\n========================\n")



# PySpark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')
sc = SparkContext(conf=conf) # create spark context with the above configuration
ssc = StreamingContext(sc, 10) # create the Streaming Context from spark context with interval size 10 seconds
ssc.checkpoint("checkpoint_TwitterApp")

# read data from TCP Port
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))

ssc.start()
ssc.awaitTermination()
