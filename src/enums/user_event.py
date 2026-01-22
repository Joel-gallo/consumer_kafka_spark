from enum import auto
from enum import Enum

#Enum que contiene una lista de posibles eventos de usuario en una aplicación web o móvil.
class UserEvent(Enum):
    # --- Sesión ---
    SESSION_START = auto()
    SESSION_END = auto()
    USER_IDLE = auto()
    USER_ACTIVE = auto()

    # --- Navegación / exploración ---
    PAGE_VIEW = auto()
    PRODUCT_VIEW = auto()
    CATEGORY_VIEW = auto()
    SEARCH = auto()
    FILTER_APPLIED = auto()
    SORT_APPLIED = auto()
    SCROLL = auto()
    SCROLL_END = auto()

    # --- Interés explícito ---
    CLICK = auto()
    CTA_CLICK = auto()
    CONTENT_EXPAND = auto()
    IMAGE_ZOOM = auto()
    VIDEO_PLAY = auto()
    VIDEO_COMPLETE = auto()
    READ_COMPLETE = auto()

    # --- Intención comercial ---
    ADD_TO_CART = auto()
    REMOVE_FROM_CART = auto()
    ADD_TO_WISHLIST = auto()
    CHECKOUT_START = auto()
    CHECKOUT_STEP_COMPLETE = auto()
    PURCHASE_COMPLETE = auto()
    PURCHASE_CANCELLED = auto()

    # --- Formularios ---
    FORM_START = auto()
    FORM_FIELD_COMPLETE = auto()
    FORM_SUBMIT = auto()
    FORM_ABANDON = auto()
    VALIDATION_ERROR = auto()

    # --- Comodidad / fricción ---
    HOVER_LONG = auto()
    RAPID_NAVIGATION = auto()
    BACK_NAVIGATION = auto()
    REPEATED_CLICK = auto()
    ERROR_SHOWN = auto()
    ERROR_RECOVERED = auto()
    RETRY_ACTION = auto()

    # --- Abandono / rechazo ---
    PAGE_EXIT = auto()
    BOUNCE = auto()
    TAB_BLUR = auto()

    # --- Confianza / aceptación ---
    COOKIE_ACCEPT = auto()
    COOKIE_REJECT = auto()
    PERMISSION_GRANTED = auto()
    PERMISSION_DENIED = auto()