"""Database models for the RESQ app."""

from typing import Any

import geopandas as gpd
from geoalchemy2 import Geometry
from loguru import logger
from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    UniqueConstraint,
    select,
    delete,
    text,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base, relationship

import views
from settings import CATEGORIES, CLUSTER_GEOPACKAGE, DB_SCHEMA, ENGINE, LABELS

Base = declarative_base(metadata=MetaData(schema=DB_SCHEMA))


DEFAULT_WEATHERS = [
    ("extreme1", "Extremes Wetter 1"),
    ("extreme2", "Extremes Wetter 2"),
    ("extreme3", "Extremes Wetter 3"),
    ("mean", "gemittelte Wetterdaten"),
]

DEFAULT_CLIMATES = [
    ("RCP8.5", "Repräsentative Konzentrationspfad mit Strahlungsantrieb von 8.5 W/m²"),
    ("RCP4.5", "Repräsentative Konzentrationspfad mit Strahlungsantrieb von 4.5 W/m²"),
    ("RCP2.6", "Repräsentative Konzentrationspfad mit Strahlungsantrieb von 2.6 W/m²"),
    ("reference", "Historisches Referenzjahr"),
]

DEFAULT_PERIODS = [
    ("P1", 2020, 2005, 2035, "Referenzjahr 2020"),
    ("P2", 2035, 2021, 2050, "Referenzjahr 2035"),
    ("P3", 2050, 2036, 2065, "Referenzjahr 2050"),
]


class Weather(Base):
    """Holds information about weather conditions."""

    __tablename__ = "weather"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)

    def __str__(self) -> str:
        """Return string representation."""
        return self.name


class Climate(Base):
    """Holds information about climate conditions."""

    __tablename__ = "climate"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)

    def __str__(self) -> str:
        """Return string representation."""
        return self.name


class Period(Base):
    """Holds information about simulation period."""

    __tablename__ = "period"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    reference_year = Column(Integer)
    period_start = Column(Integer)
    period_end = Column(Integer)
    description = Column(String)

    def __str__(self) -> str:
        """Return string representation."""
        return self.name


class Scenario(Base):
    """Holds information about a scenario."""

    __tablename__ = "scenario"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    period_id = Column(ForeignKey("period.id"), nullable=False)
    weather_id = Column(ForeignKey("weather.id"), nullable=False)
    climate_id = Column(ForeignKey("climate.id"), nullable=False)
    sensitivity_id = Column(ForeignKey("sensitivity.id"), nullable=True)

    period = relationship("Period")
    weather = relationship("Weather")
    climate = relationship("Climate")
    sensitivity = relationship("Sensitivity")

    __table_args__ = (
        Index(
            "scenario_without_sensitivity",
            period_id,
            weather_id,
            climate_id,
            unique=True,
            postgresql_where=(sensitivity_id.is_(None)),
        ),
        Index(
            "scenario_with_sensitivity",
            period_id,
            weather_id,
            climate_id,
            sensitivity_id,
            unique=True,
            postgresql_where=(sensitivity_id.is_not(None)),
        ),
    )

    def __str__(self) -> str:
        """Return string representation of scenario."""
        return self.name


class Sensitivity(Base):
    """Holds (optional) sensitivity values for a scenario."""

    __tablename__ = "sensitivity"

    id = Column(Integer, primary_key=True)
    node = Column(String)
    attribute = Column(String)
    value = Column(Float)

    def __str__(self) -> str:
        """Return string representation."""
        return f"{self.node}:{self.attribute}={self.value}"


class Cluster(Base):
    """Holds geographical information about clusters."""

    __tablename__ = "cluster"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    geometry = Column(Geometry(geometry_type="POLYGON", srid=4326))


class Sequence(Base):
    """Holds oemof timeseries results for a scenario."""

    __tablename__ = "sequence"
    __table_args__ = (
        Index(
            "sequence_unique",
            "scenario_id",
            "from_node",
            "to_node",
            "attribute",
            unique=True,
        ),
    )

    id = Column(Integer, primary_key=True)
    scenario_id = Column(ForeignKey("scenario.id", ondelete="CASCADE"), nullable=False)
    is_exogenous = Column(Boolean, nullable=False)
    from_node = Column(String)
    to_node = Column(String)
    attribute = Column(String)
    timeseries = Column(ARRAY(Float))
    total_energy = Column(Float)
    cluster_id = Column(ForeignKey("cluster.id", ondelete="SET NULL"), nullable=True)


class Scalar(Base):
    """Holds oemof scalar results for a scenario."""

    __tablename__ = "scalar"

    id = Column(Integer, primary_key=True)
    scenario_id = Column(ForeignKey("scenario.id", ondelete="CASCADE"), nullable=False)
    is_exogenous = Column(Boolean, nullable=False)
    from_node = Column(String)
    to_node = Column(String)
    attribute = Column(String)
    value = Column(Float)
    cluster_id = Column(ForeignKey("cluster.id", ondelete="SET NULL"), nullable=True)


class Label(Base):
    """Holds mappings to label components based on from/to node."""

    __tablename__ = "label"

    id = Column(Integer, primary_key=True)
    component = Column(String, unique=True, nullable=False)
    is_bus = Column(Boolean, nullable=False)
    label = Column(String, nullable=False)


class Category(Base):
    """Holds mappings to categorize components into demand/production based on from/to node."""

    __tablename__ = "category"
    __table_args__ = (UniqueConstraint("from_node", "to_node"),)

    id = Column(Integer, primary_key=True)
    from_node = Column(String, nullable=False)
    to_node = Column(String)
    category = Column(String, nullable=False)
    carrier = Column(String, nullable=False)
    is_renewable = Column(Boolean, nullable=False)


def get_or_create(
    session,  # noqa: ANN001
    model,  # noqa: ANN001
    **kwargs: Any,  # noqa: ANN401
) -> tuple[Any, bool]:
    """
    Get or create a model instance.

    Returns instance and boolean if instance already exists.
    """
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    instance = model(**kwargs)
    session.add(instance)
    session.commit()
    return instance, True


def update_default_labels() -> None:
    """Migrate labels to database."""
    with Session(ENGINE) as session:
        session.execute(delete(Label))
        logger.info("Adding default labels to the database.")
        for component, label_data in LABELS.items():
            instance = Label(
                component=component,
                label=label_data["label"],
                is_bus=label_data["bus"],
            )
            session.add(instance)

        session.commit()


def update_default_categories() -> None:
    """Migrate categories to database."""
    with Session(ENGINE) as session:
        session.execute(delete(Category))
        logger.info("Adding default categories to the database.")
        for (from_node, to_node), entries in CATEGORIES.items():
            c = Category(
                from_node=from_node,
                to_node=to_node,
                category=entries["category"],
                carrier=entries["carrier"],
                is_renewable=entries["is_renewable"],
            )
            session.add(c)
        session.commit()


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


def add_default_periods() -> None:
    """Migrate periods to database."""
    with Session(ENGINE) as session:
        logger.info("Adding default periods to the database.")
        for name, year, start, end, description in DEFAULT_PERIODS:
            p = Period(
                name=name,
                reference_year=year,
                period_start=start,
                period_end=end,
                description=description,
            )
            session.add(p)
        try:
            session.commit()
        except IntegrityError:
            logger.warning("Default periods already exist.")


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
    add_default_periods()
    update_default_labels()
    update_default_categories()
    add_clusters_from_geopackage()


def teardown_db() -> None:
    """Drop DB schema and tables."""
    logger.info("Tearing down DB tables and views.")
    views.delete_all_views()
    Base.metadata.drop_all(ENGINE)
    logger.info("Dropped database.")
