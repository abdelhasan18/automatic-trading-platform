import socket, uuid, redis
from confluent_kafka import Consumer, Producer
from models import Signal, Order

raw_ip = '10.0.0.1'

def format_bootstrap_server(ip):
    try:
        # valid IPv6 address
        socket.inet_pton(socket.AF_INET6, ip)
        return f"[{ip}]:9092"
    except socket.error:
        # IPv4 or a hostname
        return f"{ip}:9092"

KAFKA_BOOTSTRAP = format_bootstrap_server(raw_ip)

r = redis.Redis(host=raw_ip, port=6379, decode_responses=True)
consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP, 'group.id': 'risk-group', 'auto.offset.reset': 'earliest'})
consumer.subscribe(['signals'])
order_prod = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def check_risk():
    print("Risk Engine Active...")
    while True:
        msg = consumer.poll(0.1)
        
        if msg is None:
            continue

        if msg.error():
            continue

        sig = Signal.model_validate_json(msg.value())
        
        pos = int(r.get(f"pos:{sig.symbol}") or 0)
        # simple Logic: maximum of 5 positions
        if (sig.action == "BUY" and pos < 5) or (sig.action == "SELL" and pos > -5):
            ord = Order(order_id=str(uuid.uuid4()),
                        symbol=sig.symbol, 
                        action=sig.action,
                        quantity=1,
                        price_at_order=sig.price_at_signal,
                        ingestion_ts=sig.ingestion_ts
                        )
            order_prod.produce('orders', ord.model_dump_json().encode())

if __name__ == "__main__":
    check_risk()