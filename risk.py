import socket, uuid
from confluent_kafka import Consumer, Producer
from models import Signal, Order, Fill

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

consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP, 'group.id': f'risk-group-{uuid.uuid4()}', 'auto.offset.reset': 'latest'})
consumer.subscribe(['signals', 'fills'])
order_prod = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def safe_produce(producer, topic, data):
    while True:
        try:
            producer.produce(topic, data)
            producer.poll(0) 
            break
        except BufferError:
            producer.poll(0.1)

def check_risk():
    print("Risk Engine Active...")
    
    positions = {} 
    pending = {}

    while True:
        msg = consumer.poll(0.1)
        if msg is None: continue
        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue

        topic = msg.topic()
        raw_val = msg.value()
        
        try:
            if topic == 'fills':
                fill = Fill.model_validate_json(raw_val)
                sym = fill.symbol

                change = fill.quantity_filled if fill.action == "BUY" else -fill.quantity_filled
                positions[sym] = positions.get(sym, 0) + change
                
                if fill.action == "BUY":
                    pending[sym] = pending.get(sym, 0) - 1
                else:
                    pending[sym] = pending.get(sym, 0) + 1
                
                print(f" [FILL] {sym} | Confirmed Pos: {positions[sym]}")

            elif topic == 'signals':
                sig = Signal.model_validate_json(raw_val)
                sym = sig.symbol

                conf_pos = positions.get(sym, 0)
                pend_pos = pending.get(sym, 0)
                eff_pos = conf_pos + pend_pos
                
                can_buy = (sig.action == "BUY" and eff_pos < 5)
                can_sell = (sig.action == "SELL" and eff_pos > -5)

                if can_buy or can_sell:
                    if sig.action == "BUY": 
                        pending[sym] = pending.get(sym, 0) + 1
                    else: 
                        pending[sym] = pending.get(sym, 0) - 1

                    ord = Order(
                        order_id=str(uuid.uuid4()),
                        symbol=sym, 
                        action=sig.action,
                        quantity=1,
                        price_at_order=sig.price_at_signal,
                        ingestion_ts=sig.ingestion_ts
                    )

                    safe_produce(order_prod, 'orders', ord.model_dump_json().encode())
                    
                    new_eff = eff_pos + (1 if sig.action == 'BUY' else -1)
                    print(f" [ORDER] {sig.action} {sym} | New Eff Pos: {new_eff}")
                else:
                    print(f" [BLOCK] {sig.action} {sym} | Eff Pos {eff_pos} is at limit")
                    
        except Exception as e:
            print(f"Processing Error: {e}")

if __name__ == "__main__":
    check_risk()