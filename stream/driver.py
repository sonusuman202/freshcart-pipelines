
import pika
import json
import sys

if len(sys.argv) != 3:
    print("Usage: python driver.py <NAME> <BINDING_PATTERN>")
    sys.exit(1)

driver_name = sys.argv[1]
binding_pattern = sys.argv[2]

EXCHANGE = "rapido-dispatch"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()

channel.exchange_declare(
    exchange=EXCHANGE,
    exchange_type="topic",
    durable=True
)

result = channel.queue_declare(
    queue="",
    exclusive=True,
    auto_delete=True
)

queue_name = result.method.queue

channel.queue_bind(
    exchange=EXCHANGE,
    queue=queue_name,
    routing_key=binding_pattern
)

print("=" * 65)
print(f"DRIVER STARTED: {driver_name}")
print(f"Binding pattern: {binding_pattern}")
print(f"Private queue: {queue_name}")
print("=" * 65)


def callback(ch, method, properties, body):

    try:
        # Decode JSON message
        message = json.loads(body.decode("utf-8"))

        # Validate mandatory fields
        required_fields = ["id", "city", "zone", "type", "ts"]

        missing_fields = [
            field for field in required_fields
            if field not in message
        ]

        if missing_fields:
            raise ValueError(
                f"Missing required fields: {missing_fields}"
            )

        # Process valid message
        print("\nMESSAGE RECEIVED")
        print("Driver:", driver_name)
        print("Routing key:", method.routing_key)
        print("Order ID:", message["id"])
        print("City:", message["city"])
        print("Zone:", message["zone"])
        print("Type:", message["type"])
        print("Timestamp:", message["ts"])

        # Acknowledge successful processing
        ch.basic_ack(
            delivery_tag=method.delivery_tag
        )

        print("Status: ACKNOWLEDGED")

    except Exception as error:

        print("\nINVALID MESSAGE")
        print("Driver:", driver_name)
        print("Routing key:", method.routing_key)
        print("Error:", error)

        # Reject message without requeuing
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False
        )

        print("Status: NEGATIVE ACKNOWLEDGEMENT (NACK)")
        print("Requeue: False")


channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=False
)

print("\nWaiting for matching orders...\n")

try:
    channel.start_consuming()

except KeyboardInterrupt:
    print(f"\nDriver {driver_name} stopped.")

finally:
    if connection.is_open:
        connection.close()
