"""Module to handle scenarios."""

from __future__ import annotations

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

import models
import settings


def create_scenario(
    year: int,
    climate: str,
    weather: str,
    sensitivity_id: int | None = None,
) -> int:
    """Create a new scenario in the database."""
    with Session(settings.ENGINE) as session:
        climate_id = session.execute(
            select(models.Climate.id).where(models.Climate.name == climate),
        ).scalar_one_or_none()
        weather_id = session.execute(
            select(models.Weather.id).where(models.Weather.name == weather),
        ).scalar_one_or_none()
        if climate_id is None:
            raise KeyError(f"Climate '{climate}' not found in database.")
        if weather_id is None:
            raise KeyError(f"Weather '{weather}' not found in database.")

        logger.info(
            f"Creating scenario ({year=}, {weather=}, {climate=}, {sensitivity_id=})",
        )
        scenario = models.Scenario(
            year=year,
            weather_id=int(weather_id),
            climate_id=int(climate_id),
            sensitivity_id=sensitivity_id,
        )
        session.add(scenario)
        session.commit()
        logger.info(f"Created scenario #{scenario.id}.")
        return scenario.id


def delete_scenario(scenario_id: int) -> None:
    """Delete a scenario from the database."""
    with Session(settings.ENGINE) as session:
        session.delete(session.get(models.Scenario, scenario_id))
    logger.info(f"Scenario #{scenario_id} deleted from database.")


def delete_all_scenarios() -> None:
    """Delete all scenarios from the database."""
    with Session(settings.ENGINE) as session:
        session.query(models.Scenario).delete()
        session.commit()
    logger.info("All scenarios deleted from database.")


def get_cluster_for_component(component: str) -> int | None:
    """
    Look up the cluster for a component based on its name.

    Returns the cluster ID if found, None if the component is unknown, and raises
    KeyError if the mapped cluster name does not exist in the database. In case of
    any database error (e.g., missing environment, unavailable DB), also raise
    KeyError so callers get a consistent signal that the cluster cannot be found.
    """
    cluster_name = settings.COMPONENT_CLUSTERS.get(component)
    if cluster_name is None:
        return None

    with Session(settings.ENGINE) as session:
        stmt = select(models.Cluster.id).where(models.Cluster.name == cluster_name)
        result = session.execute(stmt).scalar()
        if result is None:
            error_msg = f"Cluster '{cluster_name}' for component '{component}' not found in database."
            logger.error(error_msg)
            raise KeyError(error_msg)
        return int(result)


if __name__ == "__main__":
    create_scenario(2050, "hot", "rainy")
