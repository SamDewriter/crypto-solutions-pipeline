import websockets
import json
import asyncio
import time
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()

PROJECT_ID = "crypto-solutions-437016"
TOPIC_ID = "crypto-stream"
TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{TOPIC_ID}"

def publish_message(data):
    try:
        future = publisher.publish(TOPIC_PATH, data=data.encode("utf-8"))
        print(f"Published message: {future.result()}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Binance WebSocket subscription
async def binance_ws():
    url = "wss://stream.binance.com:9443/ws"

    # Subscription parameters for Binance WebSocket
    params = {
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@ticker",
            "ethusdt@ticker",
            "bnbusdt@ticker"
        ],
        "id": 1
    }

    retries = 5  # Number of retries
    for attempt in range(retries):
        try:
            # Increase timeout duration for connection
            async with websockets.connect(url, timeout=20) as websocket:
                # Send subscription request to Binance WebSocket
                await websocket.send(json.dumps(params))
                
                # Continuously receive messages
                while True:
                    data = await websocket.recv()
                    publish_message(data)
                    print(f"Received: {data}")  # Print the data (Optional)

        except asyncio.TimeoutError:
            print(f"Connection timed out. Retrying {attempt + 1}/{retries}...")
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"An error occurred: {e}")
            break

if __name__ == "__main__":
    # Run the WebSocket connection
    asyncio.run(binance_ws())


