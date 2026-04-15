import socket, os, uuid, time
from confluent_kafka import Consumer
from models import Fill, PriceTick, MonolithicStatus
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

consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP, 'group.id': f'dashboard-group-{uuid.uuid4()}', 'auto.offset.reset': 'latest'})
consumer.subscribe(['fills', '^prices.*', 'monolithic_stats']) # regex for all price topics

def render_table(title, trades, tps, latency, portfolio, prices, event):
    print(f"\n>>> {title}")
    print(f"Total Trades: {trades} | TPS: {tps:.2f} | Relative Latency: {latency:.3f}ms")
    print("-" * 75)
    print(f"{'SYMBOL':<10} | {'POS':<8} | {'MKT PRICE':<15} | {'NET VALUE':<15}")
    print("-" * 75)
    for sym, pos in portfolio.items():
        price = prices.get(sym, 0.0)
        print(f"{sym:<10} | {pos:<8} | ${price:<14.2f} | ${pos*price:<14.2f}")
    print("-" * 75)
    print(f"Last Event: {event}")

def run_dashboard():
    SYMBOLS = [f"TICK_{i}" for i in range(20)]
    dist_p = {sym: 0 for sym in SYMBOLS}
    dist_prices = {sym: 0.0 for sym in SYMBOLS}
    dist_trades, dist_event = 0, "Waiting for data..."
    dist_lat_win = deque(maxlen=100)
    dist_min_diff = None

    mono_p = {sym: 0 for sym in SYMBOLS}
    mono_prices = {sym: 0.0 for sym in SYMBOLS}
    mono_trades, mono_event = 0, "Waiting for data..."
    mono_lat_win = deque(maxlen=100)
    mono_min_diff = None

    start_calculation_time = None
    last_ui_update = 0
    refresh_interval = 1.0 

    print("Dashboard active. Listening for Kafka messages...")

    try:
        while True:
            msg = consumer.poll(0.01)
            current_time = time.time()

            if msg is not None and not msg.error():
                topic = msg.topic()
                raw_val = msg.value().decode('utf-8')

                if start_calculation_time is None: 
                    start_calculation_time = current_time

                if topic == 'monolithic_stats':
                    ms = MonolithicStatus.model_validate_json(raw_val)
                    
                    # update monolithic state
                    mono_p, mono_prices, mono_trades, mono_event = ms.portfolio, ms.current_prices, ms.total_trades, ms.latest_event
                    
                    # Calculate relative latency
                    diff = current_time - ms.ingestion_ts
                    if mono_min_diff is None or diff < mono_min_diff: mono_min_diff = diff
                    mono_lat_win.append(max(0.1, (diff - mono_min_diff) * 1000.0))

                elif topic == 'fills':
                    f = Fill.model_validate_json(raw_val)
                    
                    dist_trades += 1
                    dist_event = f"TRADE: {f.action} {f.symbol}"
                    dist_p[f.symbol] = dist_p.get(f.symbol, 0) + (f.quantity_filled if f.action == "BUY" else -f.quantity_filled)
                    
                    # calculate relative latency
                    diff = current_time - f.ingestion_ts
                    if dist_min_diff is None or diff < dist_min_diff: dist_min_diff = diff
                    dist_lat_win.append(max(0.1, (diff - dist_min_diff) * 1000.0))

                elif topic.startswith('prices.'):
                    tick = PriceTick.model_validate_json(raw_val)
                    dist_prices[tick.symbol] = tick.price

            # ui
            if current_time - last_ui_update >= refresh_interval:
                elapsed = (current_time - start_calculation_time) if start_calculation_time else 0
                
                dist_tps = dist_trades / elapsed if elapsed > 0 else 0
                mono_tps = mono_trades / elapsed if elapsed > 0 else 0

                dist_avg_lat = sum(dist_lat_win)/len(dist_lat_win) if dist_lat_win else 0
                mono_avg_lat = sum(mono_lat_win)/len(mono_lat_win) if mono_lat_win else 0

                print("\033c", end="") 
                render_table("DISTRIBUTED SYSTEM", dist_trades, dist_tps, dist_avg_lat, dist_p, dist_prices, dist_event)
                print("\n" + "="*75)
                render_table("MONOLITHIC SYSTEM", mono_trades, mono_tps, mono_avg_lat, mono_p, mono_prices, mono_event)
                
                last_ui_update = current_time

    except KeyboardInterrupt:
        print("\nClosing...")

if __name__ == "__main__":
    run_dashboard()