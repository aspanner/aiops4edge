from fastapi import FastAPI,BackgroundTasks
from app.schemas import AnomalyRequest, BulkAnomalyRequest
from app.model import load_model, predict_anomaly, predict_bulk_anomalies,train_anomaly_model
from app.scripts.insert_redis_data import generate_random_data , insert_data_to_redis , generate_observability_event_data , insert_data_to_observability_event_redis
from redis import Redis
import redis.asyncio as aioredis
import random

# from fastapi_utils.tasks import repeat_every
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import joblib
from datetime import datetime
from contextlib import asynccontextmanager
import json
import logging
from apscheduler.schedulers.background import BackgroundScheduler

# import httpx
from httpx import AsyncClient,Timeout, ReadTimeout
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from contextlib import asynccontextmanager

import asyncio

logging.basicConfig(level=logging.DEBUG)  # Change INFO to DEBUG

logger = logging.getLogger("apscheduler")


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(redis_subscriber())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

# Initialize FastAPI app
app = FastAPI(lifespan=lifespan)

PROCESS_URL = os.environ["PROCESS_ANOMALY_URL"]
MODEL_PATH = "models/isolation_forest_model.joblib"

timeout = Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)


# Load the trained Isolation Forest model
model = load_model()
redis_host = os.getenv("REDIS_HOST", "redis")  # Use 'redis' as the default hostname
redis_port = int(os.getenv("REDIS_PORT", 6379))

# redis_obs_host = os.getenv("REDIS_OBS_HOST", "redis")  # Use 'redis' as the default hostname
# redis_obs_port = int(os.getenv("REDIS_OBS_PORT", 6379))
CHANNEL_NAME = os.getenv("CHANNEL_NAME", "observability_channel") 
REDIS_URL = f"redis://{redis_host}:{redis_port}"



# Connect to Redis
redis_client = Redis(host=redis_host, port=redis_port, decode_responses=True)

# observability_redis_client = Redis(host=redis_obs_host, port=redis_obs_port, decode_responses=True)

scheduler = AsyncIOScheduler()


@app.post("/train_model")
def start_training(background_tasks: BackgroundTasks):
    global training_in_progress

    if training_in_progress:
        return {"status": "training already in progress"}

    training_in_progress = True

    def run_training():
        global model, training_in_progress
        try:
            train_anomaly_model()
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
            logger.info("entering into process")
            tasks = [call_anomaly_api(data) for data in results]
            # asyncio.gather(*tasks)  # Run all API calls concurrently
            responses = await asyncio.gather(*tasks, return_exceptions=True)
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

@app.get("/generate-observability-data/{num_samples}")
def generate_data(num_samples: int):
    """API to generate random observability metrics data and log it"""
    data = generate_observability_event_data(num_samples)
    insert_data_to_observability_event_redis(data , redis_client)
    return data

async def call_anomaly_api(anomaly_data):
    logger.info("call call_anomaly_api")
    logger.info(f"call_anomaly_api: {PROCESS_URL}")

    try:
        #  async with httpx.Client() as client:
        async with AsyncClient() as client:

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


@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
    
@app.on_event("startup")
async def start_scheduler():
    
    
    scheduler.add_job(process_redis_data, "interval", seconds=5)
    scheduler.start()
    logger.info("Scheduler started")
    print("Scheduler started:", scheduler.running)
    
    asyncio.create_task(redis_subscriber())
    print("redis_subscriber started:")

# Test endpoint for validation
@app.get("/test")
async def test():
    return {"message": "Test endpoint is working."}

# async def consume_observability_event_redis():
#     print("Redis consumer started")
#     while True:
#         try:
#             item = await observability_redis_client.blpop("anomaly_queue")
#             if item:
#                 _, raw_data = item
#                 try:
#                     data = json.loads(raw_data)
#                     anomaly_data =convert_to_anomaly_model_format(data)
                    
#                     results = await predict_bulk_anomalies(model, [anomaly_data])
#                     anomaly = results[0]

#                     if anomaly["is_anomaly"] == "Anomaly":
#                         await redis_client.rpush("anomaly_results", json.dumps(anomaly))
#                         print(f"Anomaly detected and pushed: {anomaly}")
#                     else:
#                         print(f"Normal data: {anomaly}")
#                 except Exception as e:
#                     print(f"Error processing item: {e}")
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             await asyncio.sleep(5)  # Retry delay
            
def transform_to_anomaly_format(observability_event):
    return {
        "cluster_name": f"cluster_{random.randint(1, 10)}",
        "pod_name": observability_event["servicename"],
        "app_name": observability_event["servicename"],  # or derive differently if needed
        "cpu_usage": round(float(observability_event["cpuusage"]), 2),
        "memory_usage": round(float(observability_event["memoryusage"]), 2),
        "timestamp": observability_event["createdtime"] if isinstance(source["createdtime"], str) else observability_event["createdtime"].isoformat()
    }


def convert_to_anomaly_model_format(observability_event: dict) -> dict:
    def safe_get(key, default=None):
        return observability_event.get(key, default)

    def safe_float(val):
        try:
            return round(float(val), 2)
        except (TypeError, ValueError):
            return 0.0

    def safe_timestamp(val):
        if isinstance(val, str):
            return val
        elif isinstance(val, datetime):
            return val.isoformat()
        return datetime.utcnow().isoformat()

    return {
        "cluster_name": f"cluster_{random.randint(1, 10)}",  # Simulated/random cluster name
        "pod_name": safe_get("servicename", "unknown-pod"),
        "app_name": safe_get("servicename", "unknown-app"),  # same as pod
        "cpu_usage": safe_float(safe_get("cpuusage")),
        "memory_usage": safe_float(safe_get("memoryusage")),
        "timestamp": safe_timestamp(safe_get("createdtime"))
    }


async def redis_subscriber():
    redis = await aioredis.from_url(REDIS_URL)
    try:
        pubsub = redis.pubsub()
        
        print(f" Recieved records from  Redis.{CHANNEL_NAME}")


        await pubsub.subscribe(CHANNEL_NAME)
        
        async for message in pubsub.listen():
            print(f" Received message Redis------{message}")

            if message["type"] == "message":
                
                try:
                    data = message["data"]
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")

                    event = json.loads(data)
                    print("-----Received and converted event:------" , event)

                    formatted =   convert_to_anomaly_model_format(event)
                    anomaly = json.dumps(formatted)
                    print("Received and converted event:", formatted)
                
                    # Optional: push to another Redis queue
                    redis_client.rpush("anomaly_queue", anomaly)
                    print(f"Inserted {anomaly} records into Redis.")

                except json.JSONDecodeError:
                    # logger.error("Failed to decode JSON from message: %s", message["data"])
                    print("Failed to decode JSON from message: %s", message["data"])
                except Exception as e:
                    print("Unexpected error processing message: %s", e)


    except asyncio.CancelledError:
        # Optional: handle cleanup
        await pubsub.unsubscribe("anomaly-channel")
        raise
