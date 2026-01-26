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


def store_scenario_results(
    scenario_id: int,
    input_data: dict,
    output_data: dict,
) -> None:
    """
    Store results of a scenario into the database.

    This function processes and stores scalar and sequence data associated with a
    specific scenario by interacting with the database. Scalars and sequences are
    mapped using their attributes and related metadata such as nodes and cluster
    information. If the scenario ID provided does not exist in the database, an
    error will be raised.

    Args:
        scenario_id (int): Unique identifier of the scenario to store results for.
        input_data (dict): Dictionary containing input data for the scenario. The keys
            are tuples of from_node and to_node, and the values are dictionaries
            containing 'scalars' and 'sequences' data.
        output_data (dict): Dictionary containing result data for the scenario. The keys
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

        for is_exogenous, data in ((True, input_data), (False, output_data)):
            for (from_node, to_node), result in data.items():
                from_node_label = from_node.label
                to_node_label = to_node.label if to_node is not None else None
                cluster_id = None
                if from_node_label in settings.COMPONENT_CLUSTERS:
                    cluster_id = get_cluster_for_component(from_node_label)
                if to_node_label in settings.COMPONENT_CLUSTERS:
                    cluster_id = get_cluster_for_component(to_node_label)

                for attribute, value in result["scalars"].items():
                    if not isinstance(value, (float, int, bool)):
                        continue
                    scalar_result = models.Scalar(
                        scenario_id=scenario_id,
                        is_exogenous=is_exogenous,
                        from_node=from_node_label,
                        to_node=to_node_label,
                        attribute=attribute,
                        value=float(value),
                        cluster_id=cluster_id,
                    )
                    session.add(scalar_result)

                for attribute, series in result["sequences"].items():
                    cleaned_series = series.dropna()
                    flow = models.Sequence(
                        scenario_id=scenario_id,
                        is_exogenous=is_exogenous,
                        from_node=from_node_label,
                        to_node=to_node_label,
                        attribute=attribute,
                        timeseries=cleaned_series.tolist(),
                        total_energy=cleaned_series.sum(),
                        cluster_id=cluster_id,
                    )
                    session.add(flow)

        session.commit()
        logger.info(f"Stored results for scenario #{scenario_id}.")
