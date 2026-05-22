import pytest
from fastapi.testclient import TestClient
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orchestrator.main import app

client = TestClient(app)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_create_ticket_no_nats():
    # Тест без NATS (будет ошибка соединения)
    response = client.post("/ticket", json={
        "title": "Test title",
        "description": "Test description"
    })
    # Так как NATS не запущен, будет ошибка 500
    assert response.status_code in [500, 504]


def test_stats():
    response = client.get("/stats")
    assert response.status_code == 200
    assert "total_processed" in response.json()