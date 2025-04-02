#!/bin/bash

echo "Starting Isolation Forest Anomaly Detection API..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
