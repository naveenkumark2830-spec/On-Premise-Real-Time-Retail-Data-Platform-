import json

from kafka import KafkaProducer


class KafkaEventProducer:

    def __init__(self):

        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda value: json.dumps(value).encode("utf-8")
        )

    def send_event(self, topic, event):

        self.producer.send(topic, event)

        self.producer.flush()