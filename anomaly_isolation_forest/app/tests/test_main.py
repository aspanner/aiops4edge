mport pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "API is running and Redis data pulling is active."}

def test_get_anomalies():
    response = client.get("/anomalies")
    assert response.status_code == 200
    assert "detected_anomalies" in response.json()

# tests/test_model.py
import numpy as np
import joblib
from app.model import predict_anomaly

def test_predict_anomaly():
    model = joblib.load('app/isolation_forest_model.joblib')
    test_data = np.array([[0.5, 0.3], [10.0, 15.0]])  # One normal, one anomalous
    predictions = predict_anomaly(model, test_data)
    assert len(predictions) == 2
    assert -1 in predictions  # Anomalies should be marked as -1
