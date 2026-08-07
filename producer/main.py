import time

from data_loader import DataLoader
from event_generator import EventGenerator
from kafka_client import KafkaEventProducer


def main():

    loader = DataLoader()
    loader.load_all()

    generator = EventGenerator(loader)

    producer = KafkaEventProducer()

    print("\nStreaming Events To Kafka...\n")

    while True:

        event = generator.generate_event()

        producer.send_event("orders", event)

        print(f"Sent Order : {event['order_id']}")

        time.sleep(2)


if __name__ == "__main__":
    main()