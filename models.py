import os

from dotenv import load_dotenv
from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, String, Float, ForeignKey, ARRAY, create_engine, text, MetaData
from sqlalchemy.orm import declarative_base


load_dotenv()

DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_SCHEMA = os.environ.get("DB_SCHEMA", "resqenergy")

ENGINE = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

Base = declarative_base(metadata=MetaData(schema=DB_SCHEMA))


class Weather(Base):
    __tablename__ = "weather"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)


class Climate(Base):
    __tablename__ = "climate"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)


class Scenario(Base):
    __tablename__ = "scenario"

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    weather_id = ForeignKey("weather.id")
    climate_id = ForeignKey("climate.id")


class Sensitivity(Base):
    __tablename__ = "sensitivity"

    id = Column(Integer, primary_key=True)
    scenario_id = ForeignKey("scenario.id")
    node = Column(String)
    attribute = Column(String)
    value = Column(Float)


class Cluster(Base):
    __tablename__ = "cluster"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    geometry = Geometry(geometry_type="POLYGON", srid=4326)


class Flow(Base):
    __tablename__ = "flow"

    id = Column(Integer, primary_key=True)
    scenario_id = ForeignKey("scenario.id")
    from_node = Column(String)
    to_node = Column(String)
    timeseries = Column(ARRAY(Float))
    cluster_id = ForeignKey("cluster.id", nullable=True)


class Result(Base):
    __tablename__ = "result"

    id = Column(Integer, primary_key=True)
    scenario_id = ForeignKey("scenario.id")
    name = Column(String)
    value = Column(Float)
    cluster_id = ForeignKey("cluster.id", nullable=True)


def setup_db() -> None:
    with ENGINE.connect() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}"))
        connection.commit()
    Base.metadata.create_all(ENGINE)


if __name__ == "__main__":
    setup_db()
