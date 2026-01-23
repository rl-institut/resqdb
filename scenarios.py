"""Module to handle scenarios."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path
    from collections.abc import Iterable

import yaml
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

import models
import settings
from settings import SCENARIOS_DIR


@dataclasses.dataclass(frozen=True)
class ScenarioConfig:
    """Configuration object for a scenario."""

    name: str
    scenario: dict
    datapackage: str
    capacities: dict


def get_scenarios_in_folder(folder: Path = SCENARIOS_DIR) -> Iterable[Path]:
    """Get a list of scenarios in the given folder (defaults to scenario directory)."""
    for file in folder.iterdir():
        if file.suffix == ".yaml":
            yield file


def load_scenario_settings_from_file(filepath: str | Path) -> ScenarioConfig:
    """
    Read the scenario settings from a file.

    If only filename is given as a string, the file is read from scenario directory.
    """
    if isinstance(filepath, str):
        filepath = settings.SCENARIOS_DIR / filepath
    if not filepath.exists():
        error_msg = f"Scenario file '{filepath}' not found."
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    with filepath.open("r", encoding="utf-8") as f:
        scenario_config = yaml.safe_load(f)
    if "name" not in scenario_config:
        scenario_config["name"] = filepath.stem
    return ScenarioConfig(**scenario_config)


def create_scenario(
    period: str,
    climate: str,
    weather: str,
    sensitivity_id: int | None = None,
) -> tuple[int, bool]:
    """
    Create a new scenario in the database with given setup.

    Scenario is set up with the provided year and connected to climate, weather, and optional sensitivity identifiers.
    This function associates the given parameters with their corresponding database identifiers,
    creates a new scenario record, and commits it to the database.

    Args:
        period (str): The period associated with the scenario.
        climate (str): The name of the climate type for the scenario.
        weather (str): The name of the weather type for the scenario.
        sensitivity_id (int | None): The optional sensitivity identifier for the scenario.

    Returns:
        int: The unique identifier of the created scenario.
        bool: Whether scenario has been created or already existed.

    Raises:
        KeyError: If the specified climate or weather is not found in the database.

    """
    with Session(settings.ENGINE) as session:
        period_id = session.execute(
            select(models.Period.id).where(models.Period.name == period),
        ).scalar_one_or_none()
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

        scenario, created = models.get_or_create(
            session,
            models.Scenario,
            period_id=period_id,
            weather_id=int(weather_id),
            climate_id=int(climate_id),
            sensitivity_id=sensitivity_id,
        )
        if created:
            logger.info(f"Created scenario #{scenario.id} ({scenario}).")
        else:
            logger.warning(f"Scenario #{scenario.id} ({scenario}) already exists.")
        return scenario.id, created


def delete_scenario(scenario_id: int) -> None:
    """
    Delete a scenario from the database.

    Args:
        scenario_id (int): ID of the scenario to delete.

    """
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

    Args:
        component (str): Component to look up the cluster for.

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
