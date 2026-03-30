import socket, os
from confluent_kafka import Consumer
from models import Fill, PriceTick

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

consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP, 'group.id': 'dashboard-group', 'auto.offset.reset': 'earliest'})
consumer.subscribe(['fills', '^prices.*']) # regex for all price topics

def run_dashboard():
    portfolio = {} # {symbol: net_position}
    current_prices = {} # {symbol: last_known_price}
    total_trades = 0
    latest_event = "Waiting for data..."
    
    print("\n" + "="*50)
    print("      LIVE TRADING DASHBOARD (SIMULATED)      ")
    print("="*50 + "\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                continue

            topic = msg.topic()
            try:
                raw_val = msg.value().decode('utf-8')
                if "KAFKA_TEST_OK" in raw_val: continue
                
                if topic == 'fills':
                    fill = Fill.model_validate_json(raw_val)
                    sym = fill.symbol
                    if sym not in portfolio: portfolio[sym] = 0
                    
                    # update position logic
                    change = fill.quantity_filled if fill.action == "BUY" else -fill.quantity_filled
                    portfolio[sym] += change
                    total_trades += 1
                    latest_event = f"TRADE: {fill.action} {sym} at ${fill.fill_price:.2f}"

                elif topic.startswith('prices.'):
                    tick = PriceTick.model_validate_json(raw_val)
                    current_prices[tick.symbol] = tick.price

            except Exception:
                continue
            
            # clear screen and redraw display
            print("\033c", end="") 
            print(f"Total Trades Executed: {total_trades}")
            print("-" * 65)
            print(f"{'SYMBOL':<10} | {'POSITION':<10} | {'MARKET PRICE':<15} | {'NET VALUE':<15}")
            print("-" * 65)
            
            for sym, pos in portfolio.items():
                price = current_prices.get(sym, 0.0)
                net_value = pos * price
                print(f"{sym:<10} | {pos:<10} | ${price:<14.2f} | ${net_value:<14.2f}")
            
            print("-" * 65)
            print(f"\nLast Event: {latest_event}")
            
    except KeyboardInterrupt:
        print("Closing Dashboard...")

if __name__ == "__main__":
    run_dashboard()