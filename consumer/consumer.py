from kafka import KafkaConsumer
import os, sys, json

def log(*args, **kwargs):
    print(args, kwargs, file=sys.stderr)

def json_deserializer(data: bytes):
    return json.loads(data.decode('utf-8'))

KAFKA_BROKER = os.environ.get('KAFKA_BROKER')

consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=json_deserializer,
    auto_offset_reset='earliest'
)

for message in consumer:
    log(f'Receiving message: {message}')