from kafka import KafkaProducer
import os, sys, json, time
import requests

def log(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')  
CMC_API_KEY = os.environ.get('CMC_API_KEY', '9c15ced1-f55b-481f-9399-c7cdf6249a52')
REQUESTS_INTERVAL = 10

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)

initialized = False

# Endpoint pour récupérer les dernières listings d'exchanges
url = 'https://pro-api.coinmarketcap.com/v1/exchange/listings/latest'
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': CMC_API_KEY
}

# Paramètres de la requête
params = {
    'limit': '5',   
    'convert': 'EUR'
}

while True:
    if initialized:
        time.sleep(REQUESTS_INTERVAL)
    initialized = True

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            log(f"ErrEUR: Le servEUR a renvoyé le statut {response.status_code} : {response.text}")
            continue

        data = response.json()

        # Vérifie si data existe et est une liste
        if "data" in data and isinstance(data["data"], list):
            simplified_data = []

            for exchange_data in data["data"]:
                exchange_id = exchange_data.get("id")
                name = exchange_data.get("name")
                slug = exchange_data.get("slug")
                num_market_pairs = exchange_data.get("num_market_pairs")
                fiats = exchange_data.get("fiats", [])
                traffic_score = exchange_data.get("traffic_score")
                rank = exchange_data.get("rank")
                exchange_score = exchange_data.get("exchange_score")
                liquidity_score = exchange_data.get("liquidity_score")
                last_updated = exchange_data.get("last_updated")

                # Récupération des données de quotation en EUR
                quote = exchange_data.get("quote", {})
                EUR_data = quote.get("EUR", {})

                volume_24h = EUR_data.get("volume_24h")
                volume_24h_adjusted = EUR_data.get("volume_24h_adjusted")
                volume_7d = EUR_data.get("volume_7d")
                volume_30d = EUR_data.get("volume_30d")
                percent_change_volume_24h = EUR_data.get("percent_change_volume_24h")
                percent_change_volume_7d = EUR_data.get("percent_change_volume_7d")
                percent_change_volume_30d = EUR_data.get("percent_change_volume_30d")
                effective_liquidity_24h = EUR_data.get("effective_liquidity_24h")
                derivative_volume_EUR = EUR_data.get("derivative_volume_EUR")
                spot_volume_EUR = EUR_data.get("spot_volume_EUR")

                exchange_info = {
                    "id": exchange_id,
                    "name": name,
                    "slug": slug,
                    "num_market_pairs": num_market_pairs,
                    "fiats": fiats,
                    "traffic_score": traffic_score,
                    "rank": rank,
                    "exchange_score": exchange_score,
                    "liquidity_score": liquidity_score,
                    "last_updated": last_updated,
                    "quote": {
                        "EUR": {
                            "volume_24h": volume_24h,
                            "volume_24h_adjusted": volume_24h_adjusted,
                            "volume_7d": volume_7d,
                            "volume_30d": volume_30d,
                            "percent_change_volume_24h": percent_change_volume_24h,
                            "percent_change_volume_7d": percent_change_volume_7d,
                            "percent_change_volume_30d": percent_change_volume_30d,
                            "effective_liquidity_24h": effective_liquidity_24h,
                            "derivative_volume_EUR": derivative_volume_EUR,
                            "spot_volume_EUR": spot_volume_EUR
                        }
                    },
                    "fetched_at": time.time()
                }

                simplified_data.append(exchange_info)

            # Envoi des données sur Kafka
            message = {
                'exchanges_data': simplified_data,
                'timestamp': time.time()
            }

            producer.send('exchange_listings_latest_topic', message)
            producer.flush()
        else:
            log("Pas de champ 'data' dans la réponse ou 'data' n'est pas une liste, tentative ignorée...")

    except Exception as err:
        log(f'ErrEUR: {err}')
        continue
