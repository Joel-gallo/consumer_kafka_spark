# API Producer
Proyecto en el que se desarrolla una API que permite simular eventos de usuarios

El diseño es el siguiente
![](/diagrams/event.svg)

El stack tecnológico usado es:
- Python (FastAPI + Pydantic)

- API Consumer Kafka

- Apache Spark

- Data Lake (Parquet)

- Docker + Docker Compose

- Git

Se mantiene estructura de APIs igual que en el producer.

Job de Spark montado por el momento para ejecucion local. Posteriormente con imagenes de docker y Airflow.