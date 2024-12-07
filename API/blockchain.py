from kafka import KafkaProducer
import json
import time
import requests


def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=json_serializer
)

while True:
    API_KEY = '1122ba74-d4c9-4def-b86d-c1f2fd60b391'
    symbol = "BTC"
    reduction_rate = 50
    url = f"https://pro-api.coinmarketcap.com/v1/blockchain/statistics/latest"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": API_KEY,
    }

    try:
        
        response_data = {
            "data": {
                "BTC": {
                    "id": 1,
                    "slug": "bitcoin",
                    "symbol": "BTC",
                    "block_reward_static": 12.5,
                    "consensus_mechanism": "proof-of-work",
                    "difficulty": "11890594958796",
                    "hashrate_24h": "85116194130018810000",
                    "pending_transactions": 1177,
                    "reduction_rate": "50%",
                    "total_blocks": 595165,
                    "total_transactions": "455738994",
                    "first_block_timestamp": "2009-01-09T02:54:25.000Z"
                }
            }
        }

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
            'tps_24h': btc_data.get('tps_24h', 'N/A'),
            'timestamp': time.time()
        }

        print(f"Producing message: {message}")
        producer.send('test_topic', message)
        producer.flush()
        time.sleep(2)

    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except Exception as e:
        print(f"Unexpected error: {e}")