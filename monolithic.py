import time, uuid, random, socket
from collections import deque
from confluent_kafka import Producer
from models import PriceTick, MonolithicStatus

raw_ip = '10.0.0.1'
KAFKA_BOOTSTRAP = f"{raw_ip}:9092"
SYMBOLS = [f"TICK_{i}" for i in range(20)]

LONG_WINDOW, SHORT_WINDOW = 20, 5
price_windows = {sym: deque(maxlen=LONG_WINDOW) for sym in SYMBOLS}

portfolio = {sym: 0 for sym in SYMBOLS}
current_prices = {sym: 0.0 for sym in SYMBOLS}
total_trades = 0
latest_event = "System Initialized"

producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def run_monolithic():
    global total_trades, latest_event
    print(f"Monolithic Node Started for {len(SYMBOLS)} symbols...")
    random.seed(42)

    while True:
        for sym in SYMBOLS:
            # 1. INGESTION
            ts = time.time()
            price = round(random.uniform(0, 200), 2)
            tick = PriceTick(symbol=sym, price=price, timestamp=str(ts), ingestion_ts=ts)
            current_prices[sym] = price
            
            # 2. STRATEGY
            win = price_windows[sym]
            win.append(price)

            if len(win) == LONG_WINDOW:
                sma = sum(list(win)[-SHORT_WINDOW:]) / SHORT_WINDOW
                lma = sum(win) / LONG_WINDOW

                time.sleep(0.01) # simulate heavy computation
                action = None
                if sma > lma * 1.005: action = "BUY"
                elif sma < lma * 0.995: action = "SELL"

                if action:
                    # 3. RISK
                    eff_pos = portfolio[sym]
                    if (action == "BUY" and eff_pos < 5) or (action == "SELL" and eff_pos > -5):
                        # 4. EXECUTION
                        change = 1 if action == "BUY" else -1
                        portfolio[sym] += change
                        total_trades += 1
                        latest_event = f"MONO {action}: {sym} at ${price:.2f}"
            
            # 5. MONITOR
            if total_trades % 10 == 0: # trying not to flood the buffer with thousands of updates
                status = MonolithicStatus(
                    portfolio=portfolio,
                    current_prices=current_prices,
                    total_trades=total_trades,
                    latest_event=latest_event,
                    ingestion_ts=ts
                )
                try:
                    producer.produce('monolithic_stats', status.model_dump_json().encode())
                except BufferError:
                    producer.poll(0.5)
                    producer.produce('monolithic_stats', status.model_dump_json().encode())
        
        producer.flush()

if __name__ == "__main__":
    run_monolithic()