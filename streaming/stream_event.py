import json
import random
import time
from datetime import datetime, timedelta, timezone
from google.cloud import pubsub_v1

# GCP Project & Topic Settings
PROJECT_ID = "capstone-project-510706"
TOPIC_ID = "rides-events"

# NYC TLC Zone Ranges
MIN_ZONE = 1
MAX_ZONE = 264

# Driver & Rider Ranges (matching historical formats)
MIN_DRIVER_ID = 1
MAX_DRIVER_ID = 2000

MIN_RIDER_ID = 1
MAX_RIDER_ID = 5882


def generate_ride_event() -> dict:
    """Generates synthetic ride events matching historical schema and ID formats."""
    # Base timestamp starting Feb 1, 2018 (immediately after historical data max)
    base_time = datetime(2018, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
    random_seconds = random.randint(0, 86399)
    pickup_time = base_time + timedelta(seconds=random_seconds)
    dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 45))

    # Formatted IDs with zero padding (e.g., DRV_0001, RIDER_00001)
    driver_num = random.randint(MIN_DRIVER_ID, MAX_DRIVER_ID)
    rider_num = random.randint(MIN_RIDER_ID, MAX_RIDER_ID)

    driver_id = f"DRV_{driver_num:04d}"
    rider_id = f"RIDER_{rider_num:05d}"

    ride_id_suffix = random.randint(1000, 9999)

    return {
        "ride_id": f"ride_{pickup_time.strftime('%Y%m%d%H%M%S')}_{ride_id_suffix}",
        "rider_id": rider_id,
        "driver_id": driver_id,
        "pickup_zone": str(random.randint(MIN_ZONE, MAX_ZONE)),
        "dropoff_zone": str(random.randint(MIN_ZONE, MAX_ZONE)),
        "pickup_ts": pickup_time.isoformat(),
        "dropoff_ts": dropoff_time.isoformat(),
        "fare": round(random.uniform(10.0, 85.0), 2),
        "surge_multiplier": round(
            random.choice([1.0, 1.0, 1.0, 1.25, 1.5, 2.0]), 2
        ),
        "trip_distance": round(random.uniform(1.2, 22.5), 2),
    }


def publish_events(num_messages: int = 10, delay_seconds: float = 1.0):
    """Publishes synthetic ride events to the GCP Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Starting publisher for topic: {topic_path}\n")

    for i in range(1, num_messages + 1):
        event = generate_ride_event()
        data = json.dumps(event).encode("utf-8")

        future = publisher.publish(topic_path, data)
        message_id = future.result()

        print(
            f"[{i}/{num_messages}] Published | Driver: {event['driver_id']} | Rider: {event['rider_id']} | Ride ID: {event['ride_id']} | Msg ID: {message_id}"
        )
        time.sleep(delay_seconds)

    print("\nSuccessfully published all events.")


if __name__ == "__main__":
    publish_events(num_messages=10, delay_seconds=1.0)