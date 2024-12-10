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

    # # Si le topic choisi est "trending_topic", on affiche les données sous une forme spécifique
    # if topic == 'ohlcv_latest_topic':
    #     if 'cryptocurrencies' in data:
    #         for c in data['cryptocurrencies']:
    #             print(f"Name: {c['name']}, Symbol: {c['symbol']}, Price: {c['price']}, Market Cap: {c['market_cap']}")
    #     else:
    #         print("No cryptocurrencies data found in message.")
    
    # # Si le topic choisi est "trending_most_visited_topic", on affiche les données d'une autre forme
    # elif topic == 'trending_most_visited_topic':
    #     if 'most_visited_cryptocurrencies' in data:
    #         for c in data['most_visited_cryptocurrencies']:
    #             # Vous pouvez ajuster les informations affichées selon vos besoins
    #             print(f"Name: {c['name']}, Symbol: {c['symbol']}, Rank: {c['cmc_rank']}, Price: {c['price']}, Market Cap: {c['market_cap']}")
    #     else:
    #         print("No most_visited_cryptocurrencies data found in message.")
    
    # # Si le topic choisi est "exchange_listings_latest_topic", on affiche les données des échanges
    # elif topic == 'trending_most_visited_topic':
    #     if 'exchanges_data' in data:
    #         for e in data['exchanges_data']:
    #             EUR_data = e['quote']['EUR']
    #             print(
    #                 f"Name: {e['name']}, Slug: {e['slug']}, Rank: {e['rank']}\n"
    #                 f"Num Market Pairs: {e['num_market_pairs']}, Traffic Score: {e['traffic_score']}\n"
    #                 f"Volume 24h: {EUR_data['volume_24h']}, Volume 7d: {EUR_data['volume_7d']}, Volume 30d: {EUR_data['volume_30d']}\n"
    #                 f"Fetched At: {e['fetched_at']}, Last Updated: {e['last_updated']}\n"
    #             )
    #     else:
    #         print("No exchanges_data found in message.")
    
    
    # else:
    #     # Pour les autres topics, on affiche simplement le message brut
    #     pass
    print(f"Message reçu sur {topic}: {data}")
    
