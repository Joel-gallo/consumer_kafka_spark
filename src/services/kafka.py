from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import logging


class KafkaConsumerPingService:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def ping(self, timeout_ms: int = 3000) -> bool:
        """
        Healthcheck Kafka usando consumer.
        No consume mensajes.
        """
        consumer = None

        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=None,           
                enable_auto_commit=False,
                consumer_timeout_ms=timeout_ms,
            )

            # Fuerza metadata request
            partitions = consumer.partitions_for_topic(self.topic)
            if not partitions:
                raise RuntimeError("Topic not available")

            # Fuerza fetch offsets (comunicación real con broker). Uso de objetos TopicPartition (topic, partitionNumber)
            tps = [TopicPartition(self.topic, p) for p in partitions]
            consumer.end_offsets(tps)

            return True

        except KafkaError:
            logging.exception("Kafka consumer ping failed")
            return False

        finally:
            if consumer:
                consumer.close()
