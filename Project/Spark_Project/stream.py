import tweepy
import socket
import re


# Twitter API Credentials
ACCESS_TOKEN = "319645586-WrqQyk2St6NhvfRggQRFqYlYkmbSfro9xA5LYisB"
ACCESS_SECRET = "29QTbR7licVbQHm4uhAzkcWzJsuAxQyfyKatSk6PSDMuo"
CONSUMER_KEY = "X9mNUcNDeUYuqSKJTodPzq9Vq"
CONSUMER_SECRET = "Jg0FHA2y6oBiMcFhvpGxcW0lbG0rdNdnoSySbHk14HzFAU1Xy1"


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

hashtag = '#COVID19'
TCP_IP = 'localhost'
TCP_PORT = 52000


def preprocessing(tweet):

    # Add here your code to preprocess the tweets and  
    # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc
    
    # Regular Expression looks for emoticons, symbols, pictographs, map symbols, flags, etc.
    # A list of the emoticon unicode ranges were found at: https://stackoverflow.com/questions/33404752/removing-emojis-from-a-string-in-python
    
    regex = re.compile(pattern = "["
                        u"\U0001F600-\U0001F64F"  # emoticons
                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                        u"\U00002500-\U00002BEF"  # chinese char
                        u"\U00002702-\U000027B0"
                        u"\U00002702-\U000027B0"
                        u"\U000024C2-\U0001F251"
                        u"\U0001f926-\U0001f937"
                        u"\U00010000-\U0010ffff"
                        u"\u2640-\u2642" 
                        u"\u2600-\u2B55"
                        "]+", flags = re.UNICODE)

    return regex.sub(r'', tweet)


def getTweet(status):
    
    # You can explore fields/data other than location and the tweet itself. 
    # Check what else you could explore in terms of data inside Status object
    tweet = ""
    location = ""
    location = status.user.location

    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text

    return location, preprocessing(tweet)



# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, tweet = getTweet(status)

        if (location != None and tweet != None):
            tweetLocation = location + "::" + tweet + "\n"
            print('===================')
            print(tweetLocation)
            print('===================')
            conn.send(tweetLocation.encode('utf-8'))
        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"])


