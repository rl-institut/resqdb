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
    """
    Store the results of an oemof simulation as CSV files in the results folder.

    Results are categorized into scalars and sequences. If a folder with the specified
    datapackage name already exists and is not empty, the method skips saving the
    results unless overwriting of results is explicitly enabled.

    Args:
        datapackage_name (str): Name of the folder where results will be stored.
        results (dict): A dictionary with keys as tuples (node1, node2) and values
            as dictionaries containing 'scalars' and 'sequences' DataFrames.

    """
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
    Store results of a scenario into the database.

    This function processes and stores scalar and sequence data associated with a
    specific scenario by interacting with the database. Scalars and sequences are
    mapped using their attributes and related metadata such as nodes and cluster
    information. If the scenario ID provided does not exist in the database, an
    error will be raised.

    Args:
        scenario_id (int): Unique identifier of the scenario to store results for.
        results (dict): Dictionary containing result data for the scenario. The keys
            are tuples of from_node and to_node, and the values are dictionaries
            containing 'scalars' and 'sequences' data.

    Raises:
        ValueError: If the given scenario ID does not exist in the database.

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
                cleaned_series = series.dropna()
                flow = models.Flow(
                    scenario_id=scenario_id,
                    from_node=from_node_label,
                    to_node=to_node_label,
                    attribute=attribute,
                    timeseries=cleaned_series.tolist(),
                    cluster_id=cluster_id,
                )
                session.add(flow)

        session.commit()
        logger.info(f"Stored results for scenario #{scenario_id}.")
