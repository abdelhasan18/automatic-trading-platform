import time, json, random
from confluent_kafka import Producer
from models import PriceTick

producer = Producer({'bootstrap.servers': '10.0.0.1:9092'})
SYMBOLS = ["AAPL", "TSLA", "BTC", "ETH"]
random.seed(42)

def start_ingestion():
    print("Market Data Ingestion Started...")
    while True:
        for sym in SYMBOLS:
            tick = PriceTick(
                symbol=sym,
                price=round(random.uniform(0, 200), 2),
                timestamp=str(time.time()),
                ingestion_ts=time.time()
            )
            producer.produce(f'prices.{sym}', tick.model_dump_json().encode())
        producer.flush()
        time.sleep(0.5) # simulate 2 updates per second

if __name__ == "__main__":
    start_ingestion()