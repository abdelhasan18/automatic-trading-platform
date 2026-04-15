import time, json, random
from confluent_kafka import Producer
from models import PriceTick

KAFKA_IP = '10.0.0.1:9092'
# generate 20 unique symbols: TICK_0, TICK_1, ... TICK_19
SYMBOLS = [f"TICK_{i}" for i in range(20)]

producer = Producer({'bootstrap.servers': KAFKA_IP})
random.seed(42)

def safe_produce(producer, topic, data):
    while True:
        try:
            producer.produce(topic, data)
            producer.poll(0) 
            break
        except BufferError:
            producer.poll(0.1)

def start_stress_ingestion():
    print(f"Ingestion Node Started for {len(SYMBOLS)} symbols...")
    
    try:
        while True:
            for sym in SYMBOLS:
                ts = time.time()
                tick = PriceTick(
                    symbol=sym,
                    price=round(random.uniform(10, 500), 2),
                    timestamp=str(ts),
                    ingestion_ts=ts
                )
                safe_produce(producer, f'prices.{sym}', tick.model_dump_json().encode())
            
            # flush periodically so the buffer doesnt overflow
            producer.flush()
            time.sleep(0.05) # limit produce speed so as to not overload
    except KeyboardInterrupt:
        print("Stopping Ingestion...")

if __name__ == "__main__":
    start_stress_ingestion()