import json
import random
import time
from datetime import datetime, timezone
from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = "capstone-project-510706"
TOPIC_ID = "rides-events"

# Sample zones for synthetic data generation
ZONES = [
    "JFK Airport", "LaGuardia Airport", "Times Square", "Midtown East",
    "Financial District", "Upper East Side", "Williamsburg", "DUMBO"
]

def generate_ride_event() -> dict:
    """Generates a random ride event JSON matching the raw_ds schema."""
    ride_id_suffix = random.randint(1000, 9999)
    pickup_time = datetime.now(timezone.utc)
    # Trip duration between 5 to 45 minutes
    dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 45)) if 'timedelta' in globals() else pickup_time

    return {
        "ride_id": f"ride_{pickup_time.strftime('%Y%m%d%H%M%S')}_{ride_id_suffix}",
        "rider_id": f"rider_{random.randint(1000, 9999)}",
        "driver_id": f"driver_{random.randint(100, 999)}",
        "pickup_zone": random.choice(ZONES),
        "dropoff_zone": random.choice(ZONES),
        "pickup_ts": pickup_time.isoformat(),
        "dropoff_ts": dropoff_time.isoformat(),
        "fare": round(random.uniform(10.0, 85.0), 2),
        "surge_multiplier": round(random.choice([1.0, 1.0, 1.0, 1.25, 1.5, 2.0]), 2),
        "trip_distance": round(random.uniform(1.2, 22.5), 2),
    }

def publish_events(num_messages: int = 10, delay_seconds: float = 1.0):
    """Publishes synthetic ride events to the GCP Pub/Sub topic."""
    from datetime import timedelta
    globals()['timedelta'] = timedelta

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Starting publisher for topic: {topic_path}\n")

    for i in range(1, num_messages + 1):
        event = generate_ride_event()
        # Serialize dict to JSON string then encode to bytes
        data = json.dumps(event).encode("utf-8")
        
        # Publish message
        future = publisher.publish(topic_path, data)
        message_id = future.result()

        print(f"[{i}/{num_messages}] Published ride_id: {event['ride_id']} | Message ID: {message_id}")
        time.sleep(delay_seconds)

    print("\nSuccessfully published all events.")

if __name__ == "__main__":
    # Publish 10 messages with 1-second interval
    publish_events(num_messages=10, delay_seconds=1.0)