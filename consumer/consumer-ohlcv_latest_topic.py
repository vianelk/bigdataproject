import sys, os, json
from kafka import KafkaConsumer

def json_deserializer(data: bytes):
    return json.loads(data.decode('utf-8'))

# Get broker adress
KAFKA_BROKER = os.environ.get('KAFKA_BROKER')

# Topic to listen
topic = 'ohlcv_latest_topic'
print(f"Consommation du topic: {topic}")

# Create consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=json_deserializer,
    auto_offset_reset='earliest' # Load all messages starting from the earliest published
)

for message in consumer:
    data = message.value
    if 'cryptocurrencies' in data:
        for c in data['cryptocurrencies']:
            print(f"Name: {c['name']}, Symbol: {c['symbol']}, Price: {c['price']}, Market Cap: {c['market_cap']}")
    else:
        print("No cryptocurrencies data found in message.")
