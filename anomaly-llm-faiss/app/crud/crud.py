import json
from app.redis.redis_client import redis_client
from app.models.models import AnomalyData

def save_anomaly(anomaly: AnomalyData):
    # key = f"anomaly:{anomaly.timestamp.isoformat()}"
    print(f"finak data {anomaly}")
    # key = f"anomaly:{anomaly.get('timestamp', 'unknown')}"  

    # key = f"anomaly:{anomaly.timestamp.isoformat() if anomaly.timestamp else 'unknown'}"

    # redis_client.set(key, json.dumps(anomaly))
    return ""

def get_anomaly(key: str):
    data = redis_client.get(key)
    if data:
        return json.loads(data)
    return None
