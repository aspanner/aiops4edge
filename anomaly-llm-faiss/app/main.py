from fastapi import FastAPI, HTTPException
from app.models.models import AnomalyData
from app.crud.crud import save_anomaly, get_anomaly
from app.services.ocp_scaler import scale_pod,create_case
from app.services.rag_pipeline import analyze_anomaly_with_llm,get_vector_store
from datetime import datetime
from fastapi.encoders import jsonable_encoder

import os

from typing import Dict
app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Anomaly Detection Service is running"}
@app.post("/process-anomaly")
async def process_anomaly(anomaly_data: dict ):
    try:
        app_name = anomaly_data.get("app_name", "unknown_app")
        pod_name = anomaly_data.get("pod_name", "unknown_pod")
        cluster_info = anomaly_data.get("cluster_info", "unknown_cluster")
        response = analyze_anomaly_with_llm(anomaly_data)
        action_req = os.getenv("ACTION_REQ")
        print("scale_pod--begin")

        create_case(response,anomaly_data)
        print("scale_pod--end")
        if action_req == 'Y':
            print("scale_pod")

            scale_pod(response, app_name, 2)
            
        return {"resolution": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")
@app.post("/anomaly-detected")
async def handle_anomaly(anomaly: AnomalyData):
    anomaly_dict = anomaly.model_dump()
    
    key = save_anomaly(anomaly_dict)
    

    # Example: Scale pod if CPU anomaly is detected
    # if anomaly_dict.anomaly_type == "high_cpu_usage":
    if anomaly_dict["anomaly_type"] == "high_cpu_usage":  

        namespace = "default"
        deployment_name = "my-app-deployment"
        # scale_pod(namespace, deployment_name, replicas=2)
    return anomaly_dict
   
@app.post("/anomaly/{timestamp}")
def fetch_anomaly(timestamp: str):
    key = f"anomaly:{timestamp}"
    data = get_anomaly(key)
    if data:
        return data
    raise HTTPException(status_code=404, detail="Anomaly not found")

