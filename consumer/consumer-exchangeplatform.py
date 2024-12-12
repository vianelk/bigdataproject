from kafka import KafkaConsumer
import os
import json

def json_deserializer(data: bytes):
    return json.loads(data.decode('utf-8'))

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')

topic = 'exchange_platform_topic'
print(f"Consommation du topic: {topic}")

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=json_deserializer,
    auto_offset_reset='earliest'
)


for message in consumer:
    data = message.value
    if 'exchange_platforms' in data:
        # Trier les plateformes par volume de trading (spot_volume_usd), en gérant les valeurs None
        platforms = sorted(
            data['exchange_platforms'], 
            key=lambda x: x.get('spot_volume_usd') or 0,  # Remplace None par 0
            reverse=True
        )
        
        print("\n--- Classement des plateformes d'échange par volume de trading ---")
        for rank, platform in enumerate(platforms, start=1):
            print(f"Rang : {rank}")
            print(f"Nom : {platform.get('name', 'Nom indisponible')}")
            
            # Vérification explicite pour la description
            description = platform.get('description', 'Aucune description disponible')
            if description:
                print(f"Description : {description[:200]}...")  # Limiter à 200 caractères
            else:
                print("Description : Aucune description disponible")
            
            print(f"Date de lancement : {platform.get('date_launched', 'Non disponible')}")
            print(f"Visites hebdomadaires : {platform.get('weekly_visits', 'Non disponible')}")
            print(f"Volume de trading (USD) : {platform.get('spot_volume_usd', 'Non disponible')}")
            print(f"Frais Maker : {platform.get('maker_fee', 'Non disponible')}")
            print(f"Frais Taker : {platform.get('taker_fee', 'Non disponible')}")
            print(f"Site web : {platform.get('urls', {}).get('website', ['N/A'])[0]}")
            print("-" * 50)
    else:
        print("Aucune donnée de plateformes trouvée.")
