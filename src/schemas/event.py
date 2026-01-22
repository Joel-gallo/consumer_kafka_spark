from pydantic import BaseModel
from src.enums.device import DeviceType
from src.enums.routes import Routes
from src.enums.user_event import UserEvent

class EventBaseSchema(BaseModel):
    event_id : int
    user_id : str
    event_type : UserEvent
    event_time: str
    ip_address: str | None = None
    device: DeviceType
    location: str | None = None
    page: Routes

class EventSchema(EventBaseSchema):
    pass