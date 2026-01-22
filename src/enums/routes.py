from enum import auto
from enum import Enum

class Routes(Enum):
    HOME = "/home"
    SEARCH = "/search"
    PRODUCT_1 = "/product/1"
    PRODUCT_2 = "/product/2"
    CATEGORY_3 = "/category/3"
    CART = "/cart"
    CHECKOUT = "/checkout"