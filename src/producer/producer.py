import asyncio
import json
import websockets
from confluent_kafka import Producer
from datetime import datetime, timezone
import os
import time

# Constants
STREAM_URL = "wss://advanced-trade-ws.coinbase.com/ws"
RAW_TOPIC = os.environ.get("TRADES_TOPIC", "raw-trades")
ORDERBOOK_TOPIC = os.environ.get("ORDERBOOK_TOPIC", "raw-orderbook")
PUBLISH_INTERVAL = float(os.environ.get("PUBLISH_INTERVAL", "2.0")) # Can alter this if there are CPU issues that are causing a bottleneck

# Headers
ORDERBOOK_SUBSCRIBE_MSG = json.dumps({
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD"],
    "channel": "level2"
})

MARKET_TRADES_SUBSCRIBE_MSG = json.dumps({
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD"],
    "channel": "market_trades"
})

# Creating the Kafka producer
producer = Producer({
    "bootstrap.servers": "kafka:9092",
    "message.max.bytes": 1048576, #1MB
    "queue.buffering.max.messages": 10000,
    "batch.num.messages": 1000,
    "batch.size": 65536,
    "linger.ms": 50,
    "compression.type": "lz4"
})

def publish(topic, key, data):
    producer.produce(
        topic,
        key=str(key),
        value=json.dumps(data).encode('utf-8')
    )
    producer.poll(0)

trades_queue = asyncio.Queue(maxsize=5000) # Can adjust the queue size if there's an issue with throughput
orderbook_queue = asyncio.Queue(maxsize=5000)

# --- Trades: publish individually to match consumer's data["price"] etc ---
async def handle_trades():
    while True:
        payload = await trades_queue.get()
        ingested_at = datetime.now(timezone.utc).isoformat()

        for event in payload.get("events", []):
            for trade in event.get("trades", []):
                trade["ingested_at"] = ingested_at
                product_id = trade.get("product_id")
                publish(RAW_TOPIC, product_id, trade)  # unchanged from original

# --- Orderbook: maintain local state, publish snapshot on interval ---
async def handle_orderbook():
    order_book = {
        "BTC-USD": {"bids": {}, "asks": {}},
        "ETH-USD": {"bids": {}, "asks": {}}
    }
    last_publish = {"BTC-USD": 0.0, "ETH-USD": 0.0}

    while True:
        payload = await orderbook_queue.get()
        ingested_at = datetime.now(timezone.utc).isoformat()

        for event in payload.get("events", []):
            product_id = event.get("product_id")
            if product_id not in order_book:
                continue

            # Update local order book state
            for update in event.get("updates", []):
                price = update["price_level"]
                qty = float(update["new_quantity"])
                side = "bids" if update["side"] == "bid" else "asks"

                if qty == 0:
                    order_book[product_id][side].pop(price, None)
                else:
                    order_book[product_id][side][price] = qty

            # Publish snapshot on interval
            now = time.time()
            if now - last_publish[product_id] >= PUBLISH_INTERVAL:
                bids = sorted(
                    order_book[product_id]["bids"].items(),
                    key=lambda x: float(x[0]), reverse=True
                )[:15]
                asks = sorted(
                    order_book[product_id]["asks"].items(),
                    key=lambda x: float(x[0])
                )[:15]

                # Keep original format — consumer expects price_level and new_quantity
                publish(ORDERBOOK_TOPIC, product_id, {
                    "product_id": product_id,
                    "type": "snapshot",
                    "bids": [{"price_level": p, "new_quantity": q} for p, q in bids],
                    "asks": [{"price_level": p, "new_quantity": q} for p, q in asks],
                    "ingested_at": ingested_at
                })
                last_publish[product_id] = now

async def market_trades_event_stream():
    retry_delay = 1
    while True:
        try:
            print("Connecting to market trades...")
            async with websockets.connect(STREAM_URL, max_size=100*1024*1024) as ws:
                await ws.send(MARKET_TRADES_SUBSCRIBE_MSG)
                print("Connected!")
                retry_delay = 1
                async for message in ws:
                    try:
                        payload = json.loads(message)
                        if not trades_queue.full():
                            await trades_queue.put(payload)
                    except (json.JSONDecodeError, ValueError):
                        continue
        except (websockets.WebSocketException, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError):
                return
            print(f"Trades connection error: {exc}. Retrying in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

async def orderbook_event_stream():
    retry_delay = 1
    while True:
        try:
            print("Connecting to orderbook...")
            async with websockets.connect(STREAM_URL, max_size=100*1024*1024) as ws:
                await ws.send(ORDERBOOK_SUBSCRIBE_MSG)
                print("Connected!")
                retry_delay = 1
                async for message in ws:
                    try:
                        payload = json.loads(message)
                        if not orderbook_queue.full():
                            await orderbook_queue.put(payload)
                    except (json.JSONDecodeError, ValueError):
                        continue
        except (websockets.WebSocketException, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError):
                return
            print(f"Orderbook connection error: {exc}. Retrying in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

async def main():
    try:
        await asyncio.gather(
            market_trades_event_stream(),
            orderbook_event_stream(),
            handle_trades(),
            handle_orderbook()
        )
    except asyncio.CancelledError:
        pass
    finally:
        producer.flush()

if __name__ == "__main__":
    asyncio.run(main())