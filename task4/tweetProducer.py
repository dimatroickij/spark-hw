import tweepy
from tweepy.streaming import json
from kafka import KafkaProducer

"""

KAFKA PRODUCER INIT

"""

producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic_name = "task4"

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
        try:
            idUser = data['user']['id_str']

            screenName = data['user']['screen_name']
            print('id: %s, screen_name: %s, dateCreate: %s' % (idUser, screenName, data['created_at']))

            if idUser in idUsers:
                # put message into Kafka
                producer.send(topic_name, value=screenName.encode('UTF-8'))
        except KeyError:
            pass


"""

RUN PROCESSING

"""

# Create instance of custom listener
screenNameStreamListener = ScreenNameStreamListener()

# Set stream for twitter api with custom listener
screenNameStream = tweepy.Stream(auth=api.auth, listener=screenNameStreamListener)

# Start filtering messages
screenNameStream.filter(follow=idUsers)
