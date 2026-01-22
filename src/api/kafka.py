from fastapi import APIRouter
from src.services import kafka as kafka_service

router = APIRouter()


@router.get("/ping")
def ping():
    kafka_ok = kafka_service.ping()
    return {
        "kafka_status": "connected" if kafka_ok else "down"
    }
