from kafka import KafkaProducer
import os, sys, json, time
import requests

def log(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER')
WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)

counter = 0

while True:
    city = 'Paris'
    url = f'https://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={city}&aqi=no'
    data = requests.get(url).json()
    currentTemp = data.get('current').get('temp_c')

    message = {
        'id': counter,
        'temperature': currentTemp,
        'city': city,
        'timestamp': time.time()
    }
    log(f'Producing message {message}')

    producer.send('test_topic', message) # Send message to kafka
    producer.flush() # Wait for requests completion

    time.sleep(5)
    counter += 1