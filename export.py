"""Module to organize exports to filesystem or database."""

from __future__ import annotations

import shutil
import warnings

from loguru import logger
from sqlalchemy import exc
from sqlalchemy.orm import Session

import models
import settings
from scenarios import get_cluster_for_component


def store_results_in_folder(datapackage_name: str, results: dict) -> None:
    """Store the results as CSVs locally in the results' folder."""
    results_path = settings.RESULTS_DIR / datapackage_name

    # Check if folder already exists
    if results_path.exists() and any(results_path.iterdir()):
        if settings.OEMOF_OVERWRITE_RESULTS:
            shutil.rmtree(results_path)
        else:
            msg = (
                f"Results path {results_path} already exists. Skipping writing results."
            )
            warnings.warn(msg, stacklevel=2)
            return
    results_path.mkdir(exist_ok=True)

    # Write results as CSVs
    for nodes, data in results.items():
        if not data["scalars"].empty:
            data["scalars"].to_csv(
                results_path / f"{nodes[0]}_{nodes[1]}_scalars.csv",
                sep=";",
            )
        data["sequences"].to_csv(
            results_path / f"{nodes[0]}_{nodes[1]}_sequences.csv",
            sep=";",
        )


def store_scenario_results(scenario_id: int, results: dict) -> None:
    """
    Store results for a scenario in the database.

    Store scalars and sequences of oemof results dictionary under scenario.
    """
    with Session(settings.ENGINE) as session:
        # Check if a scenario exists
        try:
            session.get_one(models.Scenario, {"id": scenario_id})
        except exc.NoResultFound as err:
            raise ValueError(f"Scenario #{scenario_id} not found in database.") from err

        for (from_node, to_node), result in results.items():
            from_node_label = from_node.label
            to_node_label = to_node.label if to_node is not None else None
            cluster_id = None
            if from_node_label in settings.COMPONENT_CLUSTERS:
                cluster_id = get_cluster_for_component(from_node_label)
            if to_node_label in settings.COMPONENT_CLUSTERS:
                cluster_id = get_cluster_for_component(to_node_label)

            for attribute, value in result["scalars"].items():
                scalar_result = models.Result(
                    scenario_id=scenario_id,
                    from_node=from_node_label,
                    to_node=to_node_label,
                    attribute=attribute,
                    value=value,
                    cluster_id=cluster_id,
                )
                session.add(scalar_result)

            for attribute, series in result["sequences"].items():
                flow = models.Flow(
                    scenario_id=scenario_id,
                    from_node=from_node_label,
                    to_node=to_node_label,
                    attribute=attribute,
                    timeseries=series.tolist(),
                    cluster_id=cluster_id,
                )
                session.add(flow)

        session.commit()
        logger.info(f"Stored results for scenario #{scenario_id}.")
