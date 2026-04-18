import asyncio
import json
import websockets
from confluent_kafka import Producer

STREAM_URL = "wss://advanced-trade-ws.coinbase.com/ws"

SUBSCRIBE_MSG = json.dumps({
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "market_trades"
})

trade_stream: list[float] = []
trade_stream_lock = asyncio.Lock()

# Kafka producer config
producer = Producer({
    "bootstrap.servers": "kafka:9092"  # host machine connects via localhost
})

async def consume_event_stream():
    while True:
        try:
            print("Connecting...")
            async with websockets.connect(STREAM_URL) as ws:
                await ws.send(SUBSCRIBE_MSG)
                print("Connected!")
                async for message in ws:
                    try:
                        payload = json.loads(message)
                        trades = payload.get("events", [{}])[0].get("trades", [])
                        for trade in trades:
                            price = trade['price']
                            size = trade['size']
                            time = trade['time']
                            if price is not None:
                                print(f"Raw price received: {price}")
                                await process_event(float(price), float(size), str(time))
                    except (json.JSONDecodeError, ValueError):
                        continue
        except (websockets.WebSocketException, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError):
                return
            print(f"Connection error: {exc}")
            await asyncio.sleep(5)


async def process_event(value: float, size: float, time: str):
    async with trade_stream_lock:
        print(f"Trade Price: {value}, Trade Size: {size}, Trade Time: {time}")

        # Build the message payload
        message = json.dumps({
            "price": value,
            "size": size,
            "time": time
        })

        # Produce to Kafka (non-blocking)
        producer.produce(
            topic="websocket-stream",
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