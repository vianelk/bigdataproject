from kafka import KafkaProducer
import os, sys, json, time
import requests

def log(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER')
WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY')
REQUESTS_INTERVAL = 10

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)
initialized = False

while True:
    # Not waiting for first iteration
    if initialized:
        time.sleep(REQUESTS_INTERVAL)
    initialized = True

    city = 'paris'
    url = f'https://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={city}&aqi=no'

    try:
        data = requests.get(url).json() # Request data from API
        currentTemp = data['current']['temp_c'] # Get temperature field value
    except Exception as err: # Handle exceptions (just logging them)
        log(f'Error: {err}')
        continue

    # Construct message to publish to kafka
    message = {
        'temperature': currentTemp,
        'city': city,
        'timestamp': time.time()
    }
    log(f'Producing message {message}')

    try:
        producer.send('weather_topic', message) # Send message to kafka
        producer.flush() # Wait for requests completion
    except Exception as err: # Handle exceptions (just logging them)
        log(f'Error: {err}')
        continue
