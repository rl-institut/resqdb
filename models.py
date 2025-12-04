"""Database models for the RESQ app."""

import argparse
import os

from dotenv import load_dotenv
from geoalchemy2 import Geometry
from loguru import logger
from sqlalchemy import (
    ARRAY,
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    create_engine,
    text,
    select,
    Index,
)
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.exc import IntegrityError
import settings
import geopandas as gpd

load_dotenv()

DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_SCHEMA = os.environ.get("DB_SCHEMA", "resqenergy")

ENGINE = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
)

CLUSTER_GEOPACKAGE = settings.GEOPACKAGES_DIR / "clusters.gpkg"

Base = declarative_base(metadata=MetaData(schema=DB_SCHEMA))

DEFAULT_WEATHERS = [
    ("rainy", "Rainy weather"),
    ("sunny", "Sunny weather"),
    ("cloudy", "Cloudy weather"),
]

DEFAULT_CLIMATES = [
    ("hot", "Hot climate"),
    ("medium", "Medium climate"),
    ("cold", "Cold climate"),
]


class Weather(Base):
    """Holds information about weather conditions."""

    __tablename__ = "weather"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)


class Climate(Base):
    """Holds information about climate conditions."""

    __tablename__ = "climate"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)


class Scenario(Base):
    """Holds information about a scenario."""

    __tablename__ = "scenario"

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    weather_id = Column(ForeignKey("weather.id"), nullable=False)
    climate_id = Column(ForeignKey("climate.id"), nullable=False)
    sensitivity_id = Column(ForeignKey("sensitivity.id"), nullable=True)

    __table_args__ = (
        Index(
            "scenario_without_sensitivity",
            year,
            weather_id,
            climate_id,
            unique=True,
            postgresql_where=(sensitivity_id.is_(None)),
        ),
        Index(
            "scenario_with_sensitivity",
            year,
            weather_id,
            climate_id,
            sensitivity_id,
            unique=True,
            postgresql_where=(sensitivity_id.is_not(None)),
        ),
    )


class Sensitivity(Base):
    """Holds (optional) sensitivity values for a scenario."""

    __tablename__ = "sensitivity"

    id = Column(Integer, primary_key=True)
    node = Column(String)
    attribute = Column(String)
    value = Column(Float)


class Cluster(Base):
    """Holds geographical information about clusters."""

    __tablename__ = "cluster"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    geometry = Column(Geometry(geometry_type="POLYGON", srid=4326))


class Flow(Base):
    """Holds oemof timeseries results for a scenario."""

    __tablename__ = "flow"

    __table_args__ = (
        Index(
            "flow_unique",
            "scenario_id",
            "from_node",
            "to_node",
            "attribute",
            unique=True,
        ),
    )

    id = Column(Integer, primary_key=True)
    scenario_id = Column(ForeignKey("scenario.id", ondelete="CASCADE"), nullable=False)
    from_node = Column(String)
    to_node = Column(String)
    attribute = Column(String)
    timeseries = Column(ARRAY(Float))
    cluster_id = Column(ForeignKey("cluster.id"), nullable=True)


class Result(Base):
    """Holds oemof scalar results for a scenario."""

    __tablename__ = "result"

    id = Column(Integer, primary_key=True)
    scenario_id = Column(ForeignKey("scenario.id", ondelete="CASCADE"), nullable=False)
    from_node = Column(String)
    to_node = Column(String)
    attribute = Column(String)
    value = Column(Float)
    cluster_id = Column(ForeignKey("cluster.id"), nullable=True)


def add_default_weather_and_climate() -> None:
    """Add default weather and climate entries to the database."""
    logger.info("Adding default weather and climate entries to the database.")
    with Session(ENGINE) as session:
        for name, description in DEFAULT_WEATHERS:
            w = Weather(name=name, description=description)
            session.add(w)
        for name, description in DEFAULT_CLIMATES:
            c = Climate(name=name, description=description)
            session.add(c)

        try:
            session.commit()
        except IntegrityError:
            logger.warning("Default weather and climate entries already exist.")


def add_clusters_from_geopackage() -> None:
    """Add clusters from a GeoPackage to the database."""
    logger.info("Adding clusters from a GeoPackage to the database.")
    with Session(ENGINE) as session:
        select_stmt = select(Cluster.id).limit(1)
        result = session.execute(select_stmt).first()
        if result is not None:
            logger.warning("Clusters already exist in the database.")
            return

        try:
            gdf = gpd.read_file(CLUSTER_GEOPACKAGE)
        except FileNotFoundError:
            logger.error(f"GeoPackage file not found at {CLUSTER_GEOPACKAGE}")
            return

        for _, row in gdf.iterrows():
            cluster = Cluster(name=row["name"], geometry=row["geometry"].wkt)
            session.add(cluster)
        session.commit()
        logger.info(f"Added {len(gdf)} clusters from GeoPackage")


def setup_db() -> None:
    """Set up DB schema and tables from models."""
    logger.info("Setting up DB schema and tables.")
    with ENGINE.connect() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}"))
        connection.commit()
    Base.metadata.create_all(ENGINE)
    add_default_weather_and_climate()
    add_clusters_from_geopackage()


def teardown_db() -> None:
    """Drop DB schema and tables."""
    logger.info("Tearing down DB schema and tables.")
    with ENGINE.connect() as connection:
        connection.execute(text(f"DROP SCHEMA IF EXISTS {DB_SCHEMA} CASCADE"))
        connection.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="resq",
        description="Handle DB setup and teardown",
    )
    subparser = parser.add_subparsers(dest="command")
    setup = subparser.add_parser("setup")
    delete = subparser.add_parser("delete")
    args = parser.parse_args()

    if args.command == "setup":
        setup_db()
    elif args.command == "delete":
        teardown_db()
