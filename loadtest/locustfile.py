from __future__ import annotations

from locust import HttpUser, between, task


class QueryUser(HttpUser):
    wait_time = between(1, 5)

    @task
    def run_query(self) -> None:
        payload = {"query": "Show claims from active customers in last 30 days", "user_id": "locust"}
        self.client.post("/query", json=payload)
