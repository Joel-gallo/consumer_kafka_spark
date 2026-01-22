from enum import auto
from enum import Enum

class DeviceType(Enum):
    MOBILE = auto()
    TABLET = auto()
    SMARTWATCH = auto()
    DESKTOP = auto()
    SMART_TV = auto()
    OTHER = auto()