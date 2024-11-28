from kafka import KafkaProducer
import json, time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

counter = 0

while True:
    message = {
        'id': counter,
        'message': 'hello World',
        'timestamp': time.time()
    }
    print(f'Producing message {message}')
    producer.send('test_topic', message) # Send message to kafka
    producer.flush() # Wait for requests completion
    time.sleep(2)
    counter += 1