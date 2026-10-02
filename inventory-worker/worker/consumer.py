import json
import os
import signal
import asyncio
from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy.ext.asyncio import AsyncSession

from worker.db import async_session
from worker.inventory import process_order


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "inventory-worker")

running = True


def shutdown_handler(signum, frame):
    global running
    print("Shutdown signal received, stopping...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


async def process_message(message: dict) -> bool:
    async with async_session() as db:
        try:
            await process_order(db, message)
            return True
        except Exception as e:
            await db.rollback()
            print(f"Error processing message: {e}")
            return False


async def consume():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe(["order-requests"])
    print(f"Inventory worker started, consuming from order-requests...")

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            try:
                order_data = json.loads(msg.value().decode("utf-8"))
                print(f"Processing order: {order_data['order_id']}")

                success = await process_message(order_data)

                if success:
                    consumer.commit(message=msg)
                    print(f"Committed offset for order: {order_data['order_id']}")
                else:
                    print(f"Failed to process order: {order_data['order_id']}, not committing")

            except json.JSONDecodeError as e:
                print(f"Invalid JSON: {e}")
                consumer.commit(message=msg)
            except Exception as e:
                print(f"Unexpected error: {e}")

    except KafkaException as e:
        print(f"Kafka error: {e}")
    finally:
        consumer.close()
        print("Consumer closed")


if __name__ == "__main__":
    asyncio.run(consume())