import socket, uuid, time, numpy
from confluent_kafka import Consumer, Producer
from models import Order, Fill

raw_ip = '10.0.0.1'

def format_bootstrap_server(ip):
    try:
        # valid IPv6 address
        socket.inet_pton(socket.AF_INET6, ip)
        return f"[{ip}]:9092"
    except socket.error:
        # IPv4 or a hostname
        return f"{ip}:9092"

def delivery_report(err, msg):
    if err is not None:
        print(f"!!! DELIVERY FAILED: {err}")
    else:
        print(f"Confirmed fill delivered to {msg.topic()} [{msg.partition()}]")

KAFKA_BOOTSTRAP = format_bootstrap_server(raw_ip)

consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP, 'group.id': 'execution-group', 'auto.offset.reset': 'earliest'})
consumer.subscribe(['orders'])
fill_prod = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def mock_execute():
    print("Execution Node (MOCK MODE) Started...")
    print("-" * 50)
    
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            continue
        
        try:
            ord_data = msg.value()
            ord = Order.model_validate_json(ord_data)
            

            fill_msg = Fill(
                fill_id=str(uuid.uuid4()),
                symbol=ord.symbol,
                action=ord.action,
                quantity_filled=ord.quantity,
                fill_price=ord.price_at_order * numpy.random.normal(loc=1.0, scale=0.02, size=None), # simulate chaos
                ingestion_ts=ord.ingestion_ts
            )
            
            fill_prod.produce(
                'fills', 
                fill_msg.model_dump_json().encode(),
                callback=lambda err, msg: print(f"Confirmed Fill for {fill_msg.symbol}") if err is None else print(f"Error: {err}")
            )
            fill_prod.flush()
            
            latency = (time.perf_counter() - ord.ingestion_ts) * 1000
            print(f" [FILL] | {fill_msg.action} {fill_msg.symbol} | Latency: {latency:.2f}ms")
            print("-" * 50)
            
        except Exception as e:
            print(f"Execution Error: {e}")

if __name__ == "__main__":
    mock_execute()