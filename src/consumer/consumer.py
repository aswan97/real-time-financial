import json
import os
import csv
from collections import deque
from confluent_kafka import Consumer, Producer
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = "kafka:9092"
TRADES_TOPIC = os.environ.get("TRADES_TOPIC", "raw-trades")
ORDERBOOK_TOPIC = os.environ.get("ORDERBOOK_TOPIC", "raw-orderbook")
FEATURES_TOPIC = os.environ.get("FEATURES_TOPIC", "features")
OUTPUT_FILE = "/app/data/features.csv"

# Rolling windows per product — 60 trade window
windows = {
    "BTC-USD": {"prices": deque(maxlen=60), "sizes": deque(maxlen=60),
                "sides": deque(maxlen=60), "timestamps": deque(maxlen=60),
                "imbalances": deque(maxlen=60)},
    "ETH-USD": {"prices": deque(maxlen=60), "sizes": deque(maxlen=60),
                "sides": deque(maxlen=60), "timestamps": deque(maxlen=60),
                "imbalances": deque(maxlen=60)},
}

# Stores last N volatility values per product to create forward-looking label
volatility_buffer = {
    "BTC-USD": deque(maxlen=30),
    "ETH-USD": deque(maxlen=30),
}

# Latest order book state per product
orderbook = {}

feature_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "feature-engineering-group",
    "auto.offset.reset": "latest"
})
consumer.subscribe([TRADES_TOPIC, ORDERBOOK_TOPIC])

def compute_features(product_id, price, size, side):
    w = windows[product_id]

    # Pull ob_imbalance FIRST before appending to window
    ob = orderbook.get(product_id, {})
    best_bid = ob.get("best_bid", 0)
    best_ask = ob.get("best_ask", 0)
    ob_imbalance = ob.get("imbalance", 0)

    # Now safe to append — ob_imbalance is defined
    w["prices"].append(price)
    w["sizes"].append(size)
    w["sides"].append(side)
    w["timestamps"].append(datetime.now(timezone.utc).isoformat())
    w["imbalances"].append(ob_imbalance)

    prices = list(w["prices"])
    sizes = list(w["sizes"])
    sides = list(w["sides"])
    imbalances = list(w["imbalances"])

    if len(prices) < 2:
        return None

    # Price features
    returns = [(prices[i] - prices[i-1]) / prices[i-1]
               for i in range(1, len(prices))]
    price_return_1 = returns[-1]
    rolling_return = (prices[-1] - prices[0]) / prices[0]
    mean_return = sum(returns) / len(returns)
    current_volatility = (sum((r - mean_return) ** 2 for r in returns) / len(returns)) ** 0.5

    # Volatility buffer and label
    volatility_buffer[product_id].append(current_volatility)
    vol_buf = list(volatility_buffer[product_id])
    future_volatility = sum(vol_buf) / len(vol_buf) if len(vol_buf) == 30 else None
    if future_volatility is not None:
        if future_volatility < 0.0003:
            volatility_regime = 0
        elif future_volatility < 0.0008:
            volatility_regime = 1
        else:
            volatility_regime = 2
    else:
        volatility_regime = None

    # Trade flow
    buy_vol = sum(s for s, sd in zip(sizes, sides) if sd == "BUY")
    sell_vol = sum(s for s, sd in zip(sizes, sides) if sd == "SELL")
    buy_sell_ratio = buy_vol / sell_vol if sell_vol > 0 else 0
    trade_rate = len(prices)
    avg_trade_size = sum(sizes) / len(sizes)

    # VWAP
    vwap = sum(p * s for p, s in zip(prices, sizes)) / sum(sizes)
    price_vs_vwap = (price - vwap) / vwap

    # Order book — already pulled at top of function
    spread = best_ask - best_bid if best_ask and best_bid else 0
    spread_pct = spread / ((best_bid + best_ask) / 2) if best_bid and best_ask else 0

    # Rolling imbalance
    rolling_imbalance_mean = sum(imbalances) / len(imbalances)
    rolling_imbalance_std = (sum((i - rolling_imbalance_mean) ** 2
                             for i in imbalances) / len(imbalances)) ** 0.5

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "product_id": product_id,
        "last_price": price,
        "last_size": size,
        "last_side": side,
        "price_return_1": price_return_1,
        "rolling_return": rolling_return,
        "current_volatility": current_volatility,
        "buy_sell_ratio": buy_sell_ratio,
        "trade_arrival_rate": trade_rate,
        "avg_trade_size": avg_trade_size,
        "vwap": vwap,
        "price_vs_vwap": price_vs_vwap,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "spread_pct": spread_pct,
        "order_book_imbalance": ob_imbalance,
        "rolling_imbalance_mean": rolling_imbalance_mean,
        "rolling_imbalance_std": rolling_imbalance_std,
        "future_volatility": future_volatility,
        "volatility_regime": volatility_regime,
    }

def update_orderbook(msg):
    product_id = msg.get("product_id")
    if not product_id:
        return

    bids = msg.get("bids", [])
    asks = msg.get("asks", [])

    if not bids or not asks:
        return

    # Bids are pre-sorted descending, asks ascending from producer
    best_bid = float(bids[0]["price_level"])
    best_ask = float(asks[0]["price_level"])
    bid_depth = sum(float(b["new_quantity"]) for b in bids)
    ask_depth = sum(float(a["new_quantity"]) for a in asks)

    imbalance = ((bid_depth - ask_depth) / (bid_depth + ask_depth)
                 if (bid_depth + ask_depth) > 0 else 0)

    orderbook[product_id] = {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "imbalance": imbalance
    }

# CSV setup
csv_file = open(OUTPUT_FILE, "w", newline="")
csv_writer = None

print("Feature consumer running...")
while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue

    data = json.loads(msg.value().decode("utf-8"))
    topic = msg.topic()

    if topic == ORDERBOOK_TOPIC:
        update_orderbook(data)

    elif topic == TRADES_TOPIC:
        product_id = data.get("product_id")
        if product_id not in windows:
            continue

        features = compute_features(
            product_id,
            float(data["price"]),
            float(data["size"]),
            data["side"]
        )

        if features:
            # Write to CSV — skip rows where targets aren't ready yet
            if features["volatility_regime"] is not None:
                if csv_writer is None:
                    csv_writer = csv.DictWriter(csv_file, fieldnames=features.keys())
                    csv_writer.writeheader()
                csv_writer.writerow(features)
                csv_file.flush()

            # Always publish to features topic regardless of label
            feature_producer.produce(
                FEATURES_TOPIC,
                key=product_id, 
                value=json.dumps(features).encode("utf-8")
            )
            feature_producer.poll(0)

            print(
                f"{product_id} | "
                f"price: {features['last_price']:.2f} | "
                f"vol: {features['current_volatility']:.6f} | "
                f"regime: {features['volatility_regime']} | "
                f"imbalance: {features['order_book_imbalance']:.3f} | "
                f"rolling_imbalance_mean: {features['rolling_imbalance_mean']:.3f} | "
                f"rolling_imbalance_std: {features['rolling_imbalance_std']:.3f}"
            )