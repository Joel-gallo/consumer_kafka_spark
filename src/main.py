from fastapi import FastAPI
from fastapi.params import Depends
from src.api.spark import router as spark_router
from src.api.health import router as health_router
from src.api.kafka import router as kafka_router
from src.functions.depends import verify_basic_auth


app = FastAPI(title="Spark Job Orchestrator", version="0.0.7", dependencies=[Depends(verify_basic_auth)])

app.include_router(prefix="",router=spark_router,tags=["Spark Jobs"])
app.include_router(prefix="",router=kafka_router,tags=["Kafka"])
app.include_router(prefix="",router=health_router,tags=["Health Check"])