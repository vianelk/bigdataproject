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
    reduction_rate = 50
    url = f"https://pro-api.coinmarketcap.com/v2/tools/price-conversion?symbol={symbol}&amount={reduction_rate}"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": API_KEY,
    }