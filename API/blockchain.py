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

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
API_KEY = '1122ba74-d4c9-4def-b86d-c1f2fd60b391'
REQUESTS_INTERVAL = 10

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)
initialized = False

while True:
    if initialized:
        time.sleep(REQUESTS_INTERVAL)
    initialized = True

    url = "https://pro-api.coinmarketcap.com/v1/blockchain/statistics/latest"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": API_KEY,
    }

    try:
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()  
        response_data = response.json()  

        
        btc_data = response_data['data']['BTC']
        message = {
            'id': btc_data.get('id', 'N/A'),
            'slug': btc_data.get('slug', 'N/A'),
            'symbol': btc_data.get('symbol', 'N/A'),
            'block_reward_static': btc_data.get('block_reward_static', 'N/A'),
            'consensus_mechanism': btc_data.get('consensus_mechanism', 'N/A'),
            'difficulty': btc_data.get('difficulty', 'N/A'),
            'hashrate_24h': btc_data.get('hashrate_24h', 'N/A'),
            'pending_transactions': btc_data.get('pending_transactions', 'N/A'),
            'reduction_rate': btc_data.get('reduction_rate', 'N/A'),
            'total_blocks': btc_data.get('total_blocks', 'N/A'),
            'total_transactions': btc_data.get('total_transactions', 'N/A'),
            'first_block_timestamp': btc_data.get('first_block_timestamp', 'N/A'),
            'timestamp': time.time()
        }

        log(f"Producing message: {message}")

        producer.send('test_topic', message)
        producer.flush()

    except requests.exceptions.RequestException as req_err:
        log(f"Request error: {req_err}")
    except KeyError as key_err:
        log(f"Key error: {key_err}")
    except Exception as e:
        log(f"Unexpected error: {e}")
