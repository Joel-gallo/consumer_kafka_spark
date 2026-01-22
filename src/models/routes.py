from sqlalchemy import Column
from clickhouse_sqlalchemy import engines, types
from sqlalchemy.ext.declarative import declarative_base

base = declarative_base()

class Routes(base):
    __tablename__ = "routes"

    event_id = Column(types.UInt32, nullable=False)
    user_id = Column(types.String, nullable=False)
    event_type = Column(types.UInt32, nullable=False)
    event_time = Column(types.DateTime64(6), nullable=False)
    ip_address = Column(types.Nullable(types.String))
    device = Column(types.UInt16, nullable=False)
    location = Column(types.Nullable(types.String))
    page = Column(types.String, nullable=False)

    __table_args__ = (
        engines.MergeTree(
            partition_by="toYYYYMM(event_time)",
            order_by=("event_type", "event_time")
        )
    )