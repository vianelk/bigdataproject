from kafka import KafkaProducer
import os, sys, json, time

def log(*args, **kwargs):
    print(args, kwargs, file=sys.stderr)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)

counter = 0

while True:
    message = {
        'id': counter,
        'message': 'Hello World',
        'timestamp': time.time()
    }
    log(f'Producing message {message}')
    producer.send('test_topic', message) # Send message to kafka
    producer.flush() # Wait for requests completion
    time.sleep(2)
    counter += 1