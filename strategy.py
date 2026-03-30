import socket, json, uuid, time
from confluent_kafka import Consumer, Producer
from collections import deque
from models import PriceTick, Signal

LONG_WINDOW, SHORT_WINDOW = 20, 5
price_windows = {}
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

consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP, 'group.id': 'strategy-group'})
consumer.subscribe(['^prices.*'])
signal_prod = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def run_logic():
    print("Strategy Node Active...")
    while True:
        msg = consumer.poll(0.1)
        
        if msg is None:
            continue

        if msg.error():
            continue

        try:
            tick = PriceTick.model_validate_json(msg.value())
        except Exception as e:
            print(f"Skipping invalid message: {msg.value()} | Error: {e}")
            continue
        
        win = price_windows.setdefault(tick.symbol, deque(maxlen=LONG_WINDOW))
        win.append(tick.price)

        if len(win) == LONG_WINDOW:
            sma = sum(list(win)[-SHORT_WINDOW:]) / SHORT_WINDOW
            lma = sum(win) / LONG_WINDOW
            
            if sma > lma * 1.005: action = "BUY"
            elif sma < lma * 0.995: action = "SELL"
            else: continue

            sig = Signal(
                signal_id=str(uuid.uuid4()),
                symbol=tick.symbol,
                action=action,
                confidence=0.8,
                price_at_signal=tick.price,
                ingestion_ts=tick.ingestion_ts
            )
            signal_prod.produce('signals', sig.model_dump_json().encode())

if __name__ == "__main__":
    run_logic()