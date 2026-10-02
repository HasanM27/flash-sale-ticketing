import uuid

from locust import HttpUser, between, task

EVENT_ID = "bf68fd54-09a5-4bd7-aa09-6a6dc98bc25d"


class FlashSaleUser(HttpUser):
    wait_time = between(0.1, 0.3)

    @task
    def buy_ticket(self):
        self.client.post(
            "/orders",
            json={
                "event_id": EVENT_ID,
                "user_id": str(uuid.uuid4()),
                "quantity": 1,
            },
        )