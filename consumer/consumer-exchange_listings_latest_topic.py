import sys, os, json
from kafka import KafkaConsumer

def json_deserializer(data: bytes):
    return json.loads(data.decode('utf-8'))

# Get broker adress
KAFKA_BROKER = os.environ.get('KAFKA_BROKER')

# Topic to listen
topic = 'exchange_listings_latest_topic'
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
    if 'exchanges_data' in data:
        for e in data['exchanges_data']:
            EUR_data = e['quote']['EUR']
            print(
                f"Name: {e['name']}, Slug: {e['slug']}, Rank: {e['rank']}\n"
                f"Num Market Pairs: {e['num_market_pairs']}, Traffic Score: {e['traffic_score']}\n"
                f"Volume 24h: {EUR_data['volume_24h']}, Volume 7d: {EUR_data['volume_7d']}, Volume 30d: {EUR_data['volume_30d']}\n"
                f"Fetched At: {e['fetched_at']}, Last Updated: {e['last_updated']}\n"
            )
    else:
        print("No exchanges_data found in message.")
