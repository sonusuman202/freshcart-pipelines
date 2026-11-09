
import pika
import json
import random
import time
import uuid
from datetime import datetime, timezone

# RabbitMQ connection
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()

# Declare topic exchange
EXCHANGE = "rapido-dispatch"

channel.exchange_declare(
    exchange=EXCHANGE,
    exchange_type="topic",
    durable=True
)

# Random data options
cities = {
    "bangalore": [
        "koramangala",
        "indiranagar",
        "whitefield"
    ],
    "chennai": [
        "tnagar",
        "adyar",
        "velachery"
    ]
}

order_types = ["ride", "food"]

print("=" * 60)
print("RAPIDO FOOD-ORDER DISPATCHER STARTED")
print("=" * 60)

message_count = 0

try:
    while True:

        # Randomly select city, zone and type
        city = random.choice(list(cities.keys()))
        zone = random.choice(cities[city])
        order_type = random.choice(order_types)

        # Construct routing key
        routing_key = f"{city}.{zone}.{order_type}"

        # Create JSON message
        message = {
            "id": str(uuid.uuid4())[:8],
            "city": city,
            "zone": zone,
            "type": order_type,
            "ts": datetime.now(timezone.utc).isoformat()
        }

        # Publish message
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2
            )
        )

        message_count += 1

        print(
            f"[{message_count}] Published | "
            f"Routing key: {routing_key} | "
            f"Message: {message}"
        )

        # Wait 2 seconds
        time.sleep(2)

except KeyboardInterrupt:
    print("\nDispatcher stopped by user.")

finally:
    connection.close()
