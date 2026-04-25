import requests
import json 
import time 
from confluent_kafka import Producer
from datetime import datetime

# URL for NOAA 
NOAA_URL = "https://api.weather.gov/alerts/active"
HEADER = {"User-Agent": "TestApp/1.0 (test@gmail.com)"} # Required header for auth
POLL_INTERVAL = 60 # Polling every 60 seconds to not hit rate limits

# Not much else is needed besides the bootstrap server and the batch size since it's polling not a websocket stream
producer = Producer({
    "bootstrap.servers": "kafka:9092",
    "batch.size": 65536 # 64 KB
})

# Creating a set to track the event ids that we've already seen
seen_ids = set()

# Function to pull the event 
def fetch_event_alert(state=None):

    # Params for which events we'd like to pull
    params = {
        "severity": "Severe,Extreme",
        "status": "actual",
        "urgency": "Immediate,Expected"
    }

    if state:
        params['area'] = state

    response = requests.get(NOAA_URL, headers=HEADER, params=params)
    response.raise_for_status()

    # Returning the feature node in the payload
    return response.json().get("features", [])

# Function to poll and publish the response
def poll_publish():
    while True:
        alerts = fetch_event_alert()
        for alert in alerts:
            alert_id = alert['id']
            if alert_id not in seen_ids:
                seen_ids.add(alert_id)
                props = alert['properties']

                # Define the event schema
                event = {
                    "id": alert_id,
                    "event": props.get("event"),
                    "severity": props.get("severity"),
                    "urgency": props.get("urgency"),
                    "headline": props.get("headline"),
                    "areas": props.get("areaDesc"),
                    "onset": props.get("onset"),
                    "expires": props.get("expires"),
                    "description": props.get("description"),
                    "instruction": props.get("instruction"),
                    "geometry": alert.get("geometry"),  # GeoJSON polygon
                    "ingested_at": datetime.now().isoformat()
                }
                # Send the event to the producer
                producer.produce(
                    topic= "polling-weather",
                    key=str(event["id"]).encode("utf-8"),
                    value=json.dumps(event).encode("utf-8")
                )
                producer.poll(0)
                print(f"Published: {event['event']} - {event['areas']}")

        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    poll_publish()