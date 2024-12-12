from kafka import KafkaProducer
import os, sys, json, time
import requests

def log(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER')
REQUESTS_INTERVAL = 10

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)

initialized = False

# Paramètres pour l'API CoinMarketCap (sandbox)
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map'
parameters = {
  'start':'1',
  'limit':'10',
  'sort': 'cmc_rank'
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

        if "data" in data:
            # simplified_data = []
            # Print data length value
            print(f'Data has length : {len(data["data"])}')

            for item in data["data"]:
                # Get useful info
                crypto_info = {
                    "id": item["id"],
                    "name": item["name"],
                    "symbol": item["symbol"],
                    "rank": item["rank"],
                    "is_active": item["is_active"],
                    "first_historical_data": item.get("first_historical_data"),
                    "last_historical_data": item.get("last_historical_data"),
                }
                # simplified_data.append(crypto_info)

                # Create a JSON message from that info
                message = {
                    'cryptocurrency': crypto_info,
                    'timestamp': time.time()
                }
                # print(message)
                # Send message to kafka broker
                producer.send('trending_topic', message)

            # message = {
            #     'cryptocurrencies': simplified_data,
            #     'timestamp': time.time()
            # }
            # producer.send('trending_topic', message)

            # Wait for all messages to be sent
            producer.flush()
        else:
            log("No 'data' field in response, skipping...")
    except Exception as err:
        log(f'Error: {err}')
        continue
