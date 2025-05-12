from app.model import train_anomaly_model
import os

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Load the trained Isolation Forest model
model = train_anomaly_model()
