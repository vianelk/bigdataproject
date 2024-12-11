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


def top_10_by_market_cap(data):
    top_10 = sorted(data, key=lambda x: x['market_cap'], reverse=True)[:10]
    print("\n--- Top 10 des cryptomonnaies les plus capitalisées ---")
    print(f"{'Nom':<20}{'Symbole':<10}{'Capitalisation de marché (EUR)':<20}")
    print("-" * 50)
    for c in top_10:
        print(f"{c['name']:<20}{c['symbol']:<10}{c['market_cap']:<20}")


def top_10_by_volume(data):
    top_10 = sorted(data, key=lambda x: x['volume_24h'], reverse=True)[:10]
    print("\n--- Top 10 des cryptomonnaies avec le plus gros volume sur 24h ---")
    print(f"{'Nom':<20}{'Symbole':<10}{'Volume 24h (EUR)':<20}")
    print("-" * 50)
    for c in top_10:
        print(f"{c['name']:<20}{c['symbol']:<10}{c['volume_24h']:<20}")


def significant_price_changes(data):
    print("\n--- Cryptomonnaies avec variations significatives sur 24h ---")
    print(f"{'Nom':<20}{'Symbole':<10}{'Variation 24h (%)':<20}")
    print("-" * 50)
    significant_changes = [
        (c['name'], c['symbol'], c['percent_change_24h'])
        for c in data if abs(c['percent_change_24h']) > 5
    ]
    if significant_changes:
        for name, symbol, change in significant_changes:
            print(f"{name:<20}{symbol:<10}{change:<20.2f}")
    else:
        print("Aucune variation significative détectée.")


# Traitement des messages Kafka
for message in consumer:
    try:
        data = message.value.get('cryptocurrencies', [])
        timestamp = message.value.get('timestamp', 'Inconnu')

        print(f"\n--- Données reçues à {timestamp} ---")
        if data:
            top_10_by_market_cap(data)
            top_10_by_volume(data)
            significant_price_changes(data)
        else:
            print("Aucune donnée de cryptomonnaies trouvée.")
    except Exception as e:
        print(f"Erreur lors du traitement du message : {e}")
