from env_variables import KAFKA_BROKER, KAFKA_TOPIC
from src.services.spark import SparkJobService
from src.services.kafka import KafkaConsumerPingService


spark = SparkJobService()
kafka = KafkaConsumerPingService(bootstrap_servers=KAFKA_BROKER, topic= KAFKA_TOPIC)