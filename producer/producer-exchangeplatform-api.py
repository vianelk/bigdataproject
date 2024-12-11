from kafka import KafkaProducer
import os
import sys
import json
import time
import requests

def log(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
REQUESTS_INTERVAL = 10

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)

initialized = False

# Paramètres pour l'API CoinMarketCap (Exchange Info)
url = 'https://pro-api.coinmarketcap.com/v1/exchange/info'
parameters = {
    'slug': 'binance,bybit,okx,coinbase-exchange,kraken,kucoin,huobi,bitfinex,gate-io,gemini,upbit,htx'  # ID de l'exchange 
}
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': '9c15ced1-f55b-481f-9399-c7cdf6249a52', 
}

while True:
    if initialized:
        time.sleep(REQUESTS_INTERVAL)
    initialized = True

    try:
        # Passer `params=parameters` dans la requête
        response = requests.get(url, headers=headers, params=parameters)
        data = response.json()

        # Vérifiez si la réponse contient une erreur
        if "status" in data and data["status"].get("error_code") != 0:
            log(f"Erreur de l'API : {data['status'].get('error_message')}")
            continue

        # Vérifiez si "data" est présent dans la réponse
        if "data" in data:
            simplified_data = []
            for id, item in data["data"].items():
                platform = {
                    "id": id,
                    "name": item.get("name"),
                    "description": item.get("description"),
                    "date_launched": item.get("date_launched"),
                    "weekly_visits": item.get("weekly_visits"),
                    "spot_volume_usd": item.get("spot_volume_usd"),
                    "maker_fee": item.get("maker_fee"),
                    "taker_fee": item.get("taker_fee"),
                    "urls": item.get("urls")
                }
                simplified_data.append(platform)

            message = {
                'exchange_platforms': simplified_data,
                'timestamp': time.time()
            }
            producer.send('exchange_platform_topic', message)
            producer.flush()
        else:
            log("No 'data' field in response, skipping...")

    except Exception as err:
        log(f'Error: {err}')
        continue
