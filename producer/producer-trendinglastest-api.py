from kafka import KafkaProducer
import os, sys, json, time
import requests

def log(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER','localhost:9092')
REQUESTS_INTERVAL = 10

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)

initialized = False

# Paramètres pour l'API CoinMarketCap (sandbox)
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
  'start':'1',
  'limit':'10',
  'convert':'EUR'
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
        response = requests.get(url, headers=headers, params=parameters)
        data = response.json()

      
        # data["data"] contient la liste des crypto-monnaies.

        if "data" in data:
            simplified_data = []
            for item in data["data"]:
                crypto_info = {
                    "id": item["id"],
                    "name": item["name"],
                    "symbol": item["symbol"],
                    "price": item["quote"]["EUR"]["price"],
                    "market_cap": item["quote"]["EUR"]["market_cap"],
                    "volume_24h": item["quote"]["EUR"]["volume_24h"],
                    "percent_change_24h": item["quote"]["EUR"]["percent_change_24h"]
                }
                simplified_data.append(crypto_info)

            message = {
                'cryptocurrencies': simplified_data,
                'timestamp': time.time()
            }
            producer.send('trending_topic', message)
            producer.flush()
        else:
            log("No 'data' field in response, skipping...")

    except Exception as err:
        log(f'Error: {err}')
        continue
