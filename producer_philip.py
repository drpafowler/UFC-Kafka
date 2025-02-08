# producer_philip.py
import time
import random
import json
from kafka import KafkaProducer

KAFKA_TOPIC = 'ufc-fighter'
KAFKA_BROKER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def get_fighter():
    '''Get a random fighter from data/fighter_data.ndjson'''
    return random.choice(open('data/fighter_data.ndjson').readlines())

def main():
    '''Select two random fighters and send to Kafka topic'''
    while True:
        for _ in range(1):
            fighter = get_fighter()
            producer.send(KAFKA_TOPIC, value=fighter)
            print(f'Produced: {fighter}')
            time.sleep(1)
        time.sleep(15)

if __name__ == '__main__':
    main()
