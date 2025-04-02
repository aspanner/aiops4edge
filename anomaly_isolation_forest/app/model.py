# app/model.py
import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import IsolationForest
from app.utils import extract_features


MODEL_PATH = "models/isolation_forest_model.joblib"

# Function to train and save model
def train_model():
    df = pd.DataFrame({
        'cpu_usage': np.random.rand(100) * 100,
        'memory_usage': np.random.rand(100) * 1024
    })
    features = extract_features(df)
    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(features)
    joblib.dump(model, MODEL_PATH)

# Function to load model
def load_model():
    try:
        model = joblib.load(MODEL_PATH)
    except FileNotFoundError:
        train_model()
        model = joblib.load(MODEL_PATH)
    return model

# Function to make predictions
def predict_anomaly(model, request):
    data = np.array([[request.cpu_usage, request.memory_usage]])
    prediction = model.predict(data)
    result = "Anomaly" if prediction[0] == -1 else "Normal"
    return {
        "cluster_name": request.cluster_name,
        "pod_name": request.pod_name,
        "app_name": request.app_name,
        "timestamp": request.timestamp,
        "cpu_usage": request.cpu_usage,
        "memory_usage": request.memory_usage,
        "result": result
    }
def predict_bulk_anomalies(model, requests):
    
    data = [
        # [req.cpu_usage, req.memory_usage] for req in requests
        [req["cpu_usage"], req["memory_usage"]] for req in requests

    ]
    predictions = model.predict(data)
    results = []
    for req, pred in zip(requests, predictions):
        result = "Anomaly" if pred == -1 else "Normal"
        # results.append({
        #     "cluster_name": req.cluster_name,
        #     "pod_name": req.pod_name,
        #     "app_name": req.app_name,
        #     "timestamp": req.timestamp,
        #     "cpu_usage": req.cpu_usage,
        #     "memory_usage": req.memory_usage,
        #     "result": result
        # })
        results.append({
            "cluster_name": req["cluster_name"],
            "pod_name": req["pod_name"],
            "app_name": req["app_name"],
            "timestamp": req["timestamp"],
            "cpu_usage": req["cpu_usage"],
            "memory_usage": req["memory_usage"],
            "is_anomaly": result
        })
    return results
