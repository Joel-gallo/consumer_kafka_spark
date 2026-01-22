from datetime import datetime
from http.client import HTTPException

from fastapi import APIRouter

from src.enums.routes import Routes
from src.enums.device import DeviceType
from src.enums.user_event import UserEvent
from src.services import kafka_producer
from src.schemas.event import EventSchema
from env_variables import KAFKA_BROKER, KAFKA_TOPIC

router = APIRouter()

print(f'Producer sending to the broker {KAFKA_BROKER}')
print(f'Producer sending to the topic {KAFKA_TOPIC}')


tipos_de_eventos = list(UserEvent)
tipos_de_dispositivos = list(DeviceType)
rutas_posibles = list(Routes)

@router.post("/event", description="Publishes a user event to Kafka")
def publish_event(event: EventSchema):
        try:
            kafka_producer.send(KAFKA_TOPIC, event.model_dump_json(exclude=['user_id','event_type']), f'{event.user_id}:{event.event_type}')
        except RuntimeError:
            raise HTTPException(503, "Kafka not ready")

        return {"status": "accepted"}

@router.post("/events/validate")
def validate_event(event: dict):

    try:
        EventSchema.model_validate(event)
    except Exception:   
        return {
            "valid": "False"
        }

    return {
        "valid": "True"
    }