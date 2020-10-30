import tweepy
from tweepy.streaming import json
from kafka import KafkaProducer

"""
KAFKA PRODUCER INIT
"""

producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic_name = "task6"

"""
TWITTER API AUTHENTICATION
"""

consumer_token = "Slfqv88hNbuYlc35kWLcuqjXW"
consumer_secret = "pTnNG2blaFzgcXDB5lXZHOJZjgB9yMYfsr5Z7LZp9hchMjplq9"
access_token = "4196894355-pEokz8B36pgZogaPEHokkPOaY0AtRVkdSsP643d"
access_secret = "aUJ5e89fYY7t2Jh2bciC6ZnzGdeeM8pWdou86OffKRChH"

auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

idUsers = ["285532415", "147964447", "34200559", "338960856", "200036850", "72525490", "20510157", "99918629"]

"""
LISTENER TO MESSAGES FROM TWITTER
"""


class ScreenNameStreamListener(tweepy.StreamListener):
    """
    Listener Class of Twitter API Stream.
    """

    def on_data(self, raw_data):
        """Receiving a new data."""
        data = json.loads(raw_data)

        if 'in_reply_to_status_id' in data and data['in_reply_to_status_id'] is not None:
            idTweet = data['in_reply_to_status_id']
            screenName = data['in_reply_to_screen_name']
            # получение текста твита по Id
            textTweet = api.statuses_lookup([idTweet])[0].text
            if 'text' in data:
                textComment = data["text"]
            else:
                textComment = ''
            print('id: %s, screenName: %s, textTweet: %s, textComment: %s' % (idTweet, screenName, textTweet,
                                                                              textComment))
            producer.send(topic_name, value=str({"idTweet": idTweet, "screenName": screenName,
                                                 "textTweet": textTweet}).encode('utf-8'))


"""
RUN PROCESSING
"""

# Create instance of custom listener
screenNameStreamListener = ScreenNameStreamListener()

# Set stream for twitter api with custom listener
screenNameStream = tweepy.Stream(auth=api.auth, listener=screenNameStreamListener)

# Start filtering messages
screenNameStream.filter(follow=idUsers)
