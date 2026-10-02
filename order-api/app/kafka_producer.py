import json
import os
from typing import Optional
from confluent_kafka import Producer


class KafkaProducer:
    def __init__(self):
        self._producer: Optional[Producer] = None
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    def start(self):
        self._producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    def stop(self):
        if self._producer:
            self._producer.flush()

    def send_order_request(self, event_id: str, order_data: dict):
        if not self._producer:
            raise RuntimeError("Producer not started")
        self._producer.produce(
            "order-requests",
            key=event_id.encode("utf-8"),
            value=json.dumps(order_data).encode("utf-8"),
        )
        self._producer.poll(0)


kafka_producer = KafkaProducer()