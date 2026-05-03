import asyncio
import json
import websockets
from confluent_kafka import Producer
from datetime import datetime, timezone
import os

STREAM_URL = "wss://advanced-trade-ws.coinbase.com/ws"
RAW_TOPIC = os.environ.get("TRADES_TOPIC", "raw-trades")
ORDERBOOK_TOPIC = os.environ.get("ORDERBOOK_TOPIC", "raw-orderbook")

# Structure to pull the orderbook messages
ORDERBOOK_SUBSCRIBE_MSG = json.dumps({
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD"],
    "channel": "level2" 
})

# Structure to pull the market trade messages
MARKET_TRADES_SUBSCRIBE_MSG = json.dumps({
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD"],
    "channel": "market_trades" 
})

# Kafka producer config
producer = Producer({
    "bootstrap.servers": "kafka:9092",  # host machine connects via localhost
    "message.max.bytes": 5242880, # 5MB
    "queue.buffering.max.messages": 100000,
    "batch.num.messages": 1000, # Smaller batches of messages
    "batch.size": 65536, # Max batch size of 64KB
    "linger.ms": 50, # Waiting for 50ms
    "compression.type": "snappy" # Setting the compression type
})

# Function that publishes the event to kafka for each topic
def publish(topic, key, data):
    if topic == 'raw-trades':
        producer.produce(
            topic,
            key=str(key),
            value=json.dumps(data).encode('utf-8')
        )

    elif topic == 'raw-orderbook':
        producer.produce(
            topic,
            key=str(key),
            value=json.dumps(data).encode('utf-8')
        )

    # Non-blocking flush
    producer.poll(0)

# Queues to land the payloads for processing 
trades_queue = asyncio.Queue()
orderbook_queue = asyncio.Queue()

# Functions to handle each queue
async def handle_trades():
    while True:
        payload = await trades_queue.get()
        ingested_at = datetime.now(timezone.utc).isoformat()

        for event in payload.get("events", []):
            for trade in event.get("trades", []):
                trade["ingested_at"] = ingested_at
                product_id = trade.get('product_id')
                publish(RAW_TOPIC, product_id, trade)  # publishes each trade individually

async def handle_orderbook():
    while True:
        payload = await orderbook_queue.get()
        ingested_at = datetime.now(timezone.utc).isoformat()

        for event in payload.get("events", []):
            product_id = event.get("product_id")  # ← sits here
            updates = event.get("updates", [])

            bids = [u for u in updates if u["side"] == "bid"]
            asks = [u for u in updates if u["side"] == "offer"]

            # Trim to top 15 levels before publishing
            orderbook_msg = {
                "product_id": product_id,
                "type": event.get("type"),  # "snapshot" or "update"
                "bids": sorted(bids, key=lambda x: float(x["price_level"]), reverse=True)[:15],
                "asks": sorted(asks, key=lambda x: float(x["price_level"]))[:15],
                "ingested_at": ingested_at
            }

            publish(ORDERBOOK_TOPIC, product_id ,orderbook_msg)

# Separate event streams for market trades since you can't pull two channels in the same subscribe message
async def market_trades_event_stream():
    retry_delay = 1
    while True:
        try:
            print("Connecting...")
            async with websockets.connect(STREAM_URL, max_size=100*1024*1024) as ws:
                await ws.send(MARKET_TRADES_SUBSCRIBE_MSG)
                print("Connected!")
                retry_delay = 1
                async for message in ws:
                    try:
                        payload = json.loads(message)
                        await trades_queue.put(payload)
                        
                    except (json.JSONDecodeError, ValueError):
                        continue

        except (websockets.WebSocketException, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError):
                return
            print(f"Connection error trades: {exc}. Retrying in {retry_delay}s..." )
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

async def orderbook_event_stream():
    retry_delay = 1
    while True:
        try:
            print("Connecting...")
            async with websockets.connect(STREAM_URL, max_size=100*1024*1024) as ws:
                await ws.send(ORDERBOOK_SUBSCRIBE_MSG)
                print("Connected!")
                retry_delay = 1
                async for message in ws:
                    try:
                        payload = json.loads(message)
                        await orderbook_queue.put(payload)
                        
                    except (json.JSONDecodeError, ValueError):
                        continue

        except (websockets.WebSocketException, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError):
                return
            print(f"Connection error orderbook: {exc}. Retrying in {retry_delay}s..." )
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