from kafka import KafkaProducer
import os, sys, json, time
import requests, bs4

def log(*args, **kwargs):
    print(args, kwargs, file=sys.stderr)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER')
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
    url = f'https://www.weatherapi.com/weather/q/{city}'

    try:
        response = requests.get(url) # Get HTML document
        soup = bs4.BeautifulSoup(response.text, 'lxml') # Parse doc as BeautifulSoup instance
        # Find the target element (div class=weatherapi-weather-current-temp)
        currentTempTag = soup.find(
            'div',
            attrs={
                'class': 'weatherapi-weather-current-temp'
            }
        )
        currentTemp = currentTempTag.text.removesuffix(' °c') # Remove ' °c' suffix in tag text
        print(f'Current temperature: {currentTemp}') # Log temperature value
        currentTemp = float(currentTemp) # Convert temperature to float
    except Exception as err: # Handle exceptions (just logging them)
        log(f'Error: {err}')
        continue

    # Construct message to publish to kafka
    message = {
        'temperature': currentTemp,
        'city': city,
        'timestamp': time.time()
    }
    print(f'Producing message {message}') # Log message

    try:
        producer.send('test_topic', message) # Send message to kafka
        producer.flush() # Wait for requests completion
    except Exception as err: # Handle exceptions (just logging them)
        log(f'Error: {err}')
        continue
