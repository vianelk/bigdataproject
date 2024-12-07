from kafka import KafkaProducer
import json
import time
import requests
import os

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=json_serializer
)

while True:
    API_KEY = '1122ba74-d4c9-4def-b86d-c1f2fd60b391'
    symbol = "BTC"
    amount = 50
    url = f"https://pro-api.coinmarketcap.com/v2/tools/price-conversion?symbol={symbol}&amount={amount}"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": API_KEY,
    }

    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        symbol = data['data']['symbol']
        name = data['data']['name']
        amount = data['data']['amount']
        last_updated = data['data']['last_updated']

        message = {
            'symbol': symbol,
            'name': name,
            'amount': amount,
            'last_updated': last_updated,
            'timestamp': time.time()
        }
        print(f"Producing message: {message}")
        producer.send('test_topic', message) 
        producer.flush() 
        time.sleep(2) 
    except Exception as e:
        print(f"Unexpected error: {e}")
