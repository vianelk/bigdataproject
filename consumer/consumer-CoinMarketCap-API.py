import sys
import os
import json
from kafka import KafkaConsumer


def json_deserializer(data: bytes):
    return json.loads(data.decode('utf-8'))


# Adresse du broker Kafka
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')

# Nom du topic
topic = 'trending_topic'
print(f"Consommation du topic: {topic}")

# Création du consommateur Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=json_deserializer,
    auto_offset_reset='earliest'  # Lire tous les messages depuis le début
)

def top_10_by_rank(data):
    top_10 = sorted(data, key=lambda x: x['rank'])[:10]
    print("\n--- Top 10 des cryptomonnaies par rang ---")
    print(f"{'Nom':<20}{'Symbole':<10}{'Rang':<10}{'Slug':<20}")
    print("-" * 60)
    for c in top_10:
        print(f"{c['name']:<20}{c['symbol']:<10}{c['rank']:<10}{c['mslug']:<20}")

def print_all_cryptos(data):
    print("\n--- Toutes les cryptomonnaies reçues ---")
    print(f"{'Nom':<20}{'Symbole':<10}{'Rang':<10}{'Slug':<20}")
    print("-" * 60)
    for c in data:
        print(f"{c['name']:<20}{c['symbol']:<10}{c['rank']:<10}{c['mslug']:<20}")

# Traitement des messages Kafka
for message in consumer:
    try:
        data = message.value.get('cryptocurrencies', [])
        timestamp = message.value.get('timestamp', 'Inconnu')

        print(f"\n--- Données reçues à {timestamp} ---")
        if data:
            print_all_cryptos(data)
            top_10_by_rank(data)
        else:
            print("Aucune donnée de cryptomonnaies trouvée.")
    except Exception as e:
        print(f"Erreur lors du traitement du message : {e}")