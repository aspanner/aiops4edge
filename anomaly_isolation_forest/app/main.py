from fastapi import FastAPI,BackgroundTasks
from app.schemas import AnomalyRequest, BulkAnomalyRequest
from app.model import load_model, predict_anomaly, predict_bulk_anomalies
from app.scripts.insert_redis_data import generate_random_data , insert_data_to_redis
from redis import Redis
# from fastapi_utils.tasks import repeat_every
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime
from contextlib import asynccontextmanager
import json
import logging
from apscheduler.schedulers.background import BackgroundScheduler
# import httpx
from httpx import AsyncClient,Timeout, ReadTimeout
from apscheduler.schedulers.asyncio import AsyncIOScheduler


import asyncio

logging.basicConfig(level=logging.DEBUG)  # Change INFO to DEBUG
logger = logging.getLogger("apscheduler")

ogger = logging.getLogger(__name__)


# Initialize FastAPI app
app = FastAPI()

PROCESS_URL = os.environ["PROCESS_ANOMALY_URL"]

timeout = Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Configure logging with file handler and rotation
log_file = "logs/anomaly_detection.log"
file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

# Load the trained Isolation Forest model
model = load_model()
redis_host = os.getenv("REDIS_HOST", "redis")  # Use 'redis' as the default hostname
redis_port = int(os.getenv("REDIS_PORT", 6379))
# Connect to Redis
# redis_client = Redis(host='localhost', port=6379, db=0, decode_responses=True)
redis_client = Redis(host=redis_host, port=redis_port, decode_responses=True)

# MODEL_PATH = "model/isolation_forest_model.joblib"
# training_in_progress = False
PROCESS_URL = os.environ["PROCESS_ANOMALY_URL"]


@app.post("/train_model")
def start_training(background_tasks: BackgroundTasks):
    global training_in_progress

    if training_in_progress:
        return {"status": "training already in progress"}

    training_in_progress = True

    def run_training():
        global model, training_in_progress
        try:
            train_anomaly_model(MODEL_PATH)
            model = joblib.load(MODEL_PATH)
        finally:
            training_in_progress = False

    background_tasks.add_task(run_training)
    return {"status": "training started"}

@app.get("/training_status")
def get_training_status():
    return {"training": training_in_progress}


# Health check endpoint
@app.get("/")
async def root():
    return {"message": "Isolation Forest Anomaly Detection API is running."}

# Prediction endpoint
@app.post("/predict")
async def predict(request: AnomalyRequest):
    result = predict_anomaly(model, request)
    logger.info(f"Single anomaly prediction processed for pod: {request.pod_name}")
    return result

# Bulk prediction endpoint
@app.post("/predict/bulk")
async def predict_bulk(requests: BulkAnomalyRequest):
    results = predict_bulk_anomalies(model, requests.data)
    logger.info(f"Bulk anomaly prediction processed for {len(results)} entries.")
    return {"results": results}

async def process_redis_data() -> None:
    anomaly_data = []
    # print(f"Periodic Task Triggered: {datetime.datetime.now()}")  # Add this line

    while True:
        item = redis_client.lpop("anomaly_queue")  # Pull data from Redis list
        if item is None:
            break  # No more data to process
        data = json.loads(item)
        anomaly_data.append(data)

    if anomaly_data:
        logger.info(f"Processing data {anomaly_data} entries from Redis.")

        results = predict_bulk_anomalies(model, anomaly_data)
        logger.info(f"Processed {len(results)} entries from Redis.")
        logger.info(f"Processed {results} entries from Redis.")

        default_values = {
            "anomaly_type": "Unknown",
            "description": "No description available",
            "resolution": "Pending"
        }

        for res, entry in zip(results, anomaly_data):
            logger.info(f"Anomaly detected: {res['is_anomaly']} for pod {entry['pod_name']} at {entry['timestamp']}")
            
            # Ensure `timestamp` exists and is a string
            res["timestamp"] = entry.get("timestamp", datetime.utcnow().isoformat())  # Convert to ISO string
            res["anomaly_type"] = "high_cpu_usage" if res["is_anomaly"] == "Anomaly" else "Normal"
            logger.info(f"Anomaly detected-->: {res['anomaly_type'] }")

            # Merge default values
            for key, value in default_values.items():
                res.setdefault(key, value)
                    

        if results:
            logger.info("entering into rpocess")
            tasks = [call_anomaly_api(data) for data in results]
            # asyncio.gather(*tasks)  # Run all API calls concurrently
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            # for i, resp in enumerate(responses):
            #     if isinstance(resp, Exception) or resp is None:
            #         logger.error(f"Call failed for item {anomaly_data[i]}")
            #     else:
            #         logger.info(f"Call succeeded: {resp}")
            for inp, out in zip(results, responses):
                if isinstance(out, Exception):
                    logger.error("Error for %s: %s", inp, out)
                else:
                    logger.info("Success for %s: %s", inp, out)


    else:
        logger.info("No new data to process from Redis.")

    # Set up background scheduler
from datetime import datetime

def convert_datetime_to_str(data):
    """Convert all datetime fields in a dictionary to string format"""
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()  # Converts datetime to ISO 8601 string
    return data

@app.get("/generate-anomaly-data/{num_samples}")
def generate_data(num_samples: int):
    """API to generate random data and log it"""
    data = generate_random_data(num_samples)
    insert_data_to_redis(data , redis_client)
    return data

async def call_anomaly_api(anomaly_data):
    # url = os.environ["PROCESS_ANOMALY_URL"]
    logger.info("call call_anomaly_api")
    logger.info(f"call_anomaly_api: {PROCESS_URL}")

    try:
        #  async with httpx.Client() as client:
        async with AsyncClient() as client:

            # response =  client.post(PROCESS_URL, json=anomaly_data )
            # response =  client.post(PROCESS_URL, json=anomaly_data , timeout=5.0)
            response = await client.post(PROCESS_URL, json=anomaly_data, timeout=5.0)
            response.raise_for_status()

            if response.status_code == 200:
                logger.info(f"Anomaly processed: {response.json()}")

                print(f"Anomaly processed: {response.json()}")
            else:
                print(f"Error processing anomaly: {response.status_code}, {response.text}")
    except ReadTimeout:
        logger.error(f"ReadTimeout when calling {PROCESS_URL} for data {anomaly_data}")
    except Exception as e:
        logger.error(f"Failed to call anomaly API at {PROCESS_URL}: {e}")
    # except Exception as e:
    #     print(f"Failed to call anomaly API: {e}")

scheduler = AsyncIOScheduler()

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
    
@app.on_event("startup")
def start_scheduler():
    # # scheduler = BackgroundScheduler()
    # scheduler.add_job(process_redis_data, "interval", seconds=10)  # Runs every 5 minutes
    # scheduler.start()
    scheduler.add_job(process_redis_data, "interval", seconds=10)
    scheduler.start()
    logger.info("Scheduler started")

    print("Scheduler started:", scheduler.running)
# Test endpoint for validation
@app.get("/test")
async def test():
    return {"message": "Test endpoint is working."}
