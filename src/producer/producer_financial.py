import asyncio
import json
import websockets
from confluent_kafka import Producer

STREAM_URL = "wss://advanced-trade-ws.coinbase.com/ws"

SUBSCRIBE_MSG = json.dumps({
    "type": "subscribe",
    "product_ids": ["BTC-USD", "USDT-USD", "ETH-USD"],
    "channel": "level2"
})

trade_stream: list[float] = []
trade_stream_lock = asyncio.Lock()

# Kafka producer config
producer = Producer({
    "bootstrap.servers": "kafka:9092",  # host machine connects via localhost
    "queue.buffering.max.messages": 100000,
    "batch.num.messages": 1000, # Smaller batches of messages
    "batch.size": 65536, # Max batch size of 64KB
    "linger.ms": 50, # Waiting for 50ms
    "compression.type": "snappy" # Setting the compression type
})

async def consume_event_stream():
    while True:
        try:
            print("Connecting...")
            async with websockets.connect(STREAM_URL, max_size=100*1024*1024) as ws:
                await ws.send(SUBSCRIBE_MSG)
                print("Connected!")
                async for message in ws:
                    try:
                        payload = json.loads(message)
                        product_id, updates = payload.get("events", [{}])[0]['product_id'], payload.get("events", [{}])[0].get("updates", [])
                        for trade in updates:
                            price = trade['price_level']
                            quantity = trade['new_quantity']
                            time = trade['event_time']
                            if price is not None:
                                await process_event(product_id, float(price), float(quantity), str(time))
                    except (json.JSONDecodeError, ValueError):
                        continue
        except (websockets.WebSocketException, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError):
                return
            print(f"Connection error: {exc}")
            await asyncio.sleep(5)


async def process_event(key, value: float, size: float, time: str):
    async with trade_stream_lock:

        # Build the message payload
        message = json.dumps({
            "price": value,
            "size": size,
            "time": time
        })

        # Produce to Kafka (non-blocking)
        producer.produce(
            topic="websocket-stream",
            key=str(key),
            value=message.encode("utf-8")
        )
        producer.poll(0)  # Trigger delivery callbacks without blocking


async def main():
    task = asyncio.create_task(consume_event_stream())
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        producer.flush()  # Make sure all messages are delivered on shutdown


if __name__ == "__main__":
    asyncio.run(main())