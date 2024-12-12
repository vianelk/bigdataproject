import sys
import os
import json
from kafka import KafkaConsumer


def json_deserializer(data: bytes):
    return json.loads(data.decode('utf-8'))


# Adresse du broker Kafka
KAFKA_BROKER = os.environ.get('KAFKA_BROKER')

# Nom du topic
topic = 'trending_topic'
print(f"Consommation du topic: {topic}")

# Création du consommateur Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=json_deserializer,
    # auto_offset_reset='earliest'  # Lire tous les messages depuis le début
)

def top_10_by_rank(data):
    top_10 = sorted(data, key=lambda x: x['rank'])[:10]
    print("\n--- Top 10 des cryptomonnaies par rang ---")
    print(f"{'Nom':<20}{'Symbole':<10}{'Rang':<10}{'Actif':<10}{'Première Donnée':<25}{'Dernière Donnée':<25}")
    print("-" * 100)
    for c in top_10:
        print(f"{c['name']:<20}{c['symbol']:<10}{c['is_active']:<10}{c['first_historical_data']:<25}{c['last_historical_data']:<25}")

def print_all_cryptos(data):
    print("\n--- Toutes les cryptomonnaies reçues ---")
    print(f"{'Nom':<20}{'Symbole':<10}{'Rang':<10}{'Actif':<10}{'Première Donnée':<25}{'Dernière Donnée':<25}")
    print("-" * 100)
    for c in data:
        print(f"{c['name']:<20}{c['symbol']:<10}{c['is_active']:<10}{c['first_historical_data']:<25}{c['last_historical_data']:<25}")

def print_crypto(data):
    print(f"{'Nom':<20}{'Symbole':<10}{'Rang':<10}{'Actif':<10}{'Première Donnée':<25}{'Dernière Donnée':<25}")
    print("-" * 100)
    print(f"{data['name']:<20}{data['symbol']:<10}{data['is_active']:<10}{data['first_historical_data']:<25}{data['last_historical_data']:<25}")

# Traitement des messages Kafka
for message in consumer:
    try:
        # data = message.value.get('cryptocurrencies', [])
        # Get cryptocurrency field from gotten message
        crypto_info = message.value.get('cryptocurrency', {})
        # Get timestamp field from gotten message
        timestamp = message.value.get('timestamp', 'Inconnu')

        print(f"--- Données reçues à {timestamp} ---")
        print_crypto(crypto_info)

        # if data:
            # print_all_cryptos(data)
            # top_10_by_rank(data)
        # else:
        #     print("Aucune donnée de cryptomonnaies trouvée.")

        print()
    except Exception as e:
        print(f"Erreur lors du traitement du message : {e}")
