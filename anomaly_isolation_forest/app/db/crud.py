from sqlalchemy.orm import Session
from .models import Anomaly
from .schemas import AnomalyRequest
import uuid
from datetime import datetime
from sqlalchemy import and_



def create_anomaly(db: Session, anomaly: AnomalyRequest):
    db_anomaly = Anomaly(
        id=str(uuid.uuid4()),
        cluster_name=anomaly.cluster_name,
        pod_name=anomaly.pod_name,
        app_name=anomaly.app_name,
        timestamp=anomaly.timestamp,
        cpu_usage=anomaly.cpu_usage,
        memory_usage=anomaly.memory_usage,
        is_anomaly=anomaly.is_anomaly
    )
    db.add(db_anomaly)
    db.commit()
    db.refresh(db_anomaly)
    return db_anomaly

def get_all_anomalies(db: Session):
    return db.query(Anomaly).order_by(Anomaly.timestamp.desc()).all()

def get_all_anomalies_sorted(db: Session, skip: int = 0, limit: int = 100):
    return (
        db.query(Anomaly)
        .order_by(Anomaly.timestamp.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

def get_anomalies_between_dates(
    db: Session,
    start_date: datetime,
    end_date: datetime,
    skip: int = 0,
    limit: int = 100
):
    return (
        db.query(Anomaly)
        .filter(
            and_(
                Anomaly.timestamp >= start_date,
                Anomaly.timestamp <= end_date
            )
        )
        .order_by(Anomaly.timestamp.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

def get_anomalies_filtered(
    db: Session,
    start_date: datetime,
    end_date: datetime,
    cluster_name: str = None,
    pod_name: str = None,
    app_name: str = None,
    skip: int = 0,
    limit: int = 100
):
    filters = [
        Anomaly.timestamp >= start_date,
        Anomaly.timestamp <= end_date,
    ]
    if cluster_name:
        filters.append(Anomaly.cluster_name == cluster_name)
    if pod_name:
        filters.append(Anomaly.pod_name == pod_name)
    if app_name:
        filters.append(Anomaly.app_name == app_name)

    return (
        db.query(Anomaly)
        .filter(and_(*filters))
        .order_by(Anomaly.timestamp.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )
