# tests/test_api.py
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

# Test health check endpoint
def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Isolation Forest Anomaly Detection API is running."}

# Test prediction endpoint with normal input
def test_predict_normal():
    payload = {
        "cluster_name": "cluster1",
        "pod_name": "pod1",
        "app_name": "app1",
        "cpu_usage": 50.0,
        "memory_usage": 500.0,
        "timestamp": "2025-02-25T14:00:00"
    }
    response = client.post("/predict", json=payload)
    assert response.status_code == 200
    assert "result" in response.json()

# Test prediction endpoint with anomaly input
def test_predict_anomaly():
    payload = {
        "cluster_name": "cluster2",
        "pod_name": "pod2",
        "app_name": "app2",
        "cpu_usage": 99.0,
        "memory_usage": 1024.0,
        "timestamp": "2025-02-25T15:00:00"
    }
    response = client.post("/predict", json=payload)
    assert response.status_code == 200
    assert "result" in response.json()

# tests/test_api.py

# Test bulk prediction
def test_bulk_predict():
    payload = {
        "data": [
            {
                "cluster_name": "cluster1",
                "pod_name": "pod1",
                "app_name": "app1",
                "cpu_usage": 30.0,
                "memory_usage": 256.0,
                "timestamp": "2025-02-25T16:00:00"
            },
            {
                "cluster_name": "cluster2",
                "pod_name": "pod2",
                "app_name": "app2",
                "cpu_usage": 90.0,
                "memory_usage": 1024.0,
                "timestamp": "2025-02-25T17:00:00"
            }
        ]
    }
    response = client.post("/predict/bulk", json=payload)
    assert response.status_code == 200
    assert "results" in response.json()
    assert len(response.json()["results"]) == 2
