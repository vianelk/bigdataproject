import sys, os, json
from kafka import KafkaConsumer

def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

KAFKA_BROKER = os.environ.get('KAFKA_BROKER')

# Demande à l'utilisateur quel topic écouter
topic = 'trending_topic'

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=json_deserializer,
    auto_offset_reset='earliest'
)

print(f"Consommation du topic: {topic}")

for message in consumer:
    data = message.value
    print(f"Message reçu sur {topic}: {data}")
    
