# app/schemas.py
from pydantic import BaseModel
from typing import List

class AnomalyRequest(BaseModel):
    cluster_name: str
    pod_name: str
    app_name: str
    cpu_usage: float
    memory_usage: float
    timestamp: str

class BulkAnomalyRequest(BaseModel):
    data: List[AnomalyRequest]
