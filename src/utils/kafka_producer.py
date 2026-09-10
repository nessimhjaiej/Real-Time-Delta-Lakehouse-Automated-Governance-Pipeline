import json
import logging
import random
import time
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_TOPIC = "ecommerce.orders.v1"
BOOTSTRAP_SERVERS = ["localhost:9092"]

PRODUCTS = [(1, 25.50), (2, 120.00), (3, 15.99), (4, 45.00), (5, 8.99)]


def generate_order_event(invoice_num: int) -> dict:
    """Generates a single streaming order transaction event."""
    prod_id, price = random.choice(PRODUCTS)
    cust_num = random.randint(100, 150)
    return {
        "invoice_id": f"INV-STREAM-{invoice_num}",
        "customer_id": f"CUST-{cust_num}",
        "customer_name": f"User_{cust_num}",
        "email": f"user_{cust_num}@example.com",
        "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
        "product_id": str(prod_id),
        "quantity": random.randint(1, 5),
        "unit_price": price,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


def start_producer() -> None:
    """Publishes continuous JSON event payloads to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    logger.info(f"Kafka Producer initialized. Streaming to topic: '{KAFKA_TOPIC}'...")

    invoice_counter = 5000
    try:
        while True:
            event = generate_order_event(invoice_counter)
            producer.send(KAFKA_TOPIC, value=event)
            logger.info(f"Published event: {event['invoice_id']} - {event['email']}")
            invoice_counter += 1
            time.sleep(2)  # Stream an event every 2 seconds
    except KeyboardInterrupt:
        logger.info("Stopping Kafka Producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    start_producer()