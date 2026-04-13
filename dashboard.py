import socket, os, uuid, time
from confluent_kafka import Consumer
from models import Fill, PriceTick
from collections import deque

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

consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP, 'group.id': f'dashboard-group-{uuid.uuid4()}', 'auto.offset.reset': 'earliest'})
consumer.subscribe(['fills', '^prices.*']) # regex for all price topics

def run_dashboard():
    portfolio = {}      # {symbol: net_position}
    current_prices = {} # {symbol: last_market_price}
    total_trades = 0
    latest_event = "Waiting for data..."
    
    latency_window = deque(maxlen=20)
    min_observed_diff = None

    start_time = time.time() - 3600 

    print("\n" + "="*50)
    print("      LIVE TRADING DASHBOARD (SIMULATED)      ")
    print("="*50 + "\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None or msg.error():
                continue

            topic = msg.topic()
            raw_val = msg.value().decode('utf-8')

            if topic == 'fills':
                fill = Fill.model_validate_json(raw_val)
                
                # filters for burn-in ticks
                if fill.ingestion_ts < start_time:
                    continue
                
                arrival_time = time.time()
                raw_diff = arrival_time - fill.ingestion_ts
                
                # set the baseline on the first message or a new fastest message
                if min_observed_diff is None or raw_diff < min_observed_diff:
                    min_observed_diff = raw_diff
                
                # latency is the additional time spent beyond the established baseline
                relative_latency_ms = (raw_diff - min_observed_diff) * 1000.0
                latency_window.append(max(0.1, relative_latency_ms))
                
                sym = fill.symbol
                current_pos = portfolio.get(sym, 0)
                change = fill.quantity_filled if fill.action == "BUY" else -fill.quantity_filled
                portfolio[sym] = current_pos + change
                
                total_trades += 1
                latest_event = f"TRADE: {fill.action} {sym} at ${fill.fill_price:.2f}"

            elif topic.startswith('prices.'):
                tick = PriceTick.model_validate_json(raw_val)
                current_prices[tick.symbol] = tick.price

            # calculate moving average
            avg_latency = sum(latency_window) / len(latency_window) if latency_window else 0.0
            
            # ui Rendering
            print("\033c", end="")
            print(f"Total Trades: {total_trades} | Relative Latency: {avg_latency:.3f}ms")
            print("-" * 75)
            print(f"{'SYMBOL':<10} | {'POS':<8} | {'MKT PRICE':<15} | {'NET VALUE':<15}")
            print("-" * 75)
            
            for sym, pos in portfolio.items():
                price = current_prices.get(sym, 0.0)
                net_value = pos * price
                print(f"{sym:<10} | {pos:<8} | ${price:<14.2f} | ${net_value:<14.2f}")
            
            print("-" * 75)
            print(f"\nLast Event: {latest_event}")
            
    except KeyboardInterrupt:
        print("\nClosing Dashboard...")

if __name__ == "__main__":
    run_dashboard()