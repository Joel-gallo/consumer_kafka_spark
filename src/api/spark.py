from fastapi import APIRouter, HTTPException
from src.services import spark as spark_service

router = APIRouter()


@router.post("/run")
def run_job():
    try:
        spark_service.run()
        return {"status": "running"}
    except Exception as exc:
        raise HTTPException(400, str(exc))


@router.post("/cancel")
def cancel_job():
    try:
        spark_service.cancel()
        return {"status": "cancelled"}
    except Exception as exc:
        raise HTTPException(400, str(exc))


@router.get("/status")
def status():
    return {
        "status": spark_service.status,
        "last_run": spark_service.last_run,
    }
