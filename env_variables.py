import os
from dotenv import load_dotenv

load_dotenv()

def load_users() -> dict[str, str]:
    users = {}
    raw = os.getenv("BASIC_USERS", "")

    for pair in raw.split(","):
        if not pair:
            continue
        username, password = pair.split(":", 1)
        users[username] = password

    return users

KAFKA_BROKER = os.getenv("KAFKA_BROKER", []).split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "")

SPARK_MASTER = os.getenv('SPARK_MASTER', "")

WAREHOUSE_PATH = os.getenv('WAREHOUSE_PATH', '')

USERS = load_users()
