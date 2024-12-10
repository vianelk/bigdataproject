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

# Paramètres pour l'API CoinMarketCap (most visited)
url = 'https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/trending/most-visited'
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': '9c15ced1-f55b-481f-9399-c7cdf6249a52',
}

# Paramètres de requête
params = {
    'start': 1,
    'limit': 100,
    'time_period': '24h',
    'convert': 'EUR'
}

while True:
    if initialized:
        time.sleep(REQUESTS_INTERVAL)
    initialized = True

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            log(f"Erreur: Le serveur a renvoyé le statut {response.status_code} : {response.text}")
            continue

        data = response.json()

        # Vérification du champ "data"
        if "data" in data and isinstance(data["data"], list):
            simplified_data = []
            for item in data["data"]:
                # On s'assure que le champ quote et EUR existent
                quote = item.get("quote", {})
                EUR = quote.get("EUR", {})
                if not EUR:
                    # Si EUR n'existe pas, on ignore cet item
                    log(f"Avertissement: Pas de données EUR pour {item.get('name', 'inconnu')}, item ignoré.")
                    continue

                # Construction de l'objet crypto simplifié
                crypto_info = {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "symbol": item.get("symbol"),
                    "price": EUR.get("price"),
                    "cmc_rank": item.get("cmc_rank")
                }
                simplified_data.append(crypto_info)

            # Création du message à envoyer à Kafka
            message = {
                'cryptocurrencies': simplified_data,
                'timestamp': time.time()
            }

            producer.send('trending_most_visited_topic', message)
            producer.flush()
        else:
            log("Pas de champ 'data' dans la réponse ou 'data' n'est pas une liste, tentative ignorée...")

    except Exception as err:
        log(f'Erreur: {err}')
        continue
