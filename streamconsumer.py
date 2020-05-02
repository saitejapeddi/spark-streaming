from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from textblob import TextBlob
es = Elasticsearch()
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def main():

    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter")
    for msg in consumer:

        data = json.loads(msg.value)
        sA = SentimentIntensityAnalyzer()

        tweet = TextBlob(data["text"])
        polarity = sA.polarity_scores(tweet)
        if (polarity["compound"] > 0):
            data["polarity"]= "positive"
        elif (polarity["compound"] < 0):
            data["polarity"]=  "negative"
        else:
            data["polarity"]=  "neutral"

        print(data["text"] + data["polarity"]+str(polarity))
        # add text and sentiment info to elasticsearch
        es.index(index="tweet",
                  doc_type="test-type",
                  body={"author": data["user"]["screen_name"],
                        "time": data["created_at"],
                        "Positivity": data["polarity"],
                        "message": data["text"]})
        print('\n')

if __name__ == "__main__":
    main()