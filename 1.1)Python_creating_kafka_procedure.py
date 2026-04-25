import requests
from datetime import datetime
import time
import json
from kafka import KafkaProducer

# რამდენიმე კრიპტო ვალუტა
crypto_ids = "bitcoin,ethereum,dogecoin"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "Coin-stream"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

print("Crypto Producer started... (Press Ctrl+C to stop)")

while True:
    # API request
    params = {
        "ids": crypto_ids,
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true"
    }
    response = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params=params
    )

    # If the rate limit is exceeded, wait and retry.

    if response.status_code == 429:
        print("Rate limit exceeded, waiting 60 seconds...")
        time.sleep(60)
        continue

    data = response.json()

    for crypto in crypto_ids.split(","):
        if crypto in data:
            info = data[crypto]
            price = info.get("usd", None)
            market_cap = info.get("usd_market_cap", None)
            vol_24h = info.get("usd_24h_vol", None)
            change_24h = info.get("usd_24h_change", None)
            last_updated_ts = info.get("last_updated_at", None)
            last_updated = datetime.fromtimestamp(last_updated_ts).strftime('%Y-%m-%d %H:%M:%S') if last_updated_ts else None

            #print(f"--- {crypto.capitalize()} ---")
            #print(f"Price: ${price:,.2f}")
            #print(f"Market Cap: ${market_cap:,.2f}")
            #print(f"24h Volume: ${vol_24h:,.2f}")
            #print(f"24h Change: {change_24h:.2f}%")
            #print(f"Last Updated: {last_updated}\n")

            # Create an event JSON for Kafka
            event = {
                "crypto": crypto,
                "price": price,
                "market_cap": market_cap,
                "24h_volume": vol_24h,
                "24h_change": change_24h,
                "last_updated": last_updated
            }

            # Send to a Kafka topic

            producer.send(KAFKA_TOPIC, value=event)

        else:
            print(f"{crypto.capitalize()} data not available.")

    # Wait 60 seconds before the next request.
    time.sleep(60)
