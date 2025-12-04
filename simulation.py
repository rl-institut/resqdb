"""Module to run oemof simulations and store results."""

import warnings
import shutil

from oemof.solph import EnergySystem, Model, processing
from oemof.tabular import datapackage  # noqa: F401
from oemof.tabular.constraint_facades import CONSTRAINT_TYPE_MAP
from oemof.tabular.facades import TYPEMAP

import settings


def simulate_datapackage(datapackage_name: str) -> dict:
    """Simulate a datapackage and return the results."""
    datapackage_json = str(
        settings.DATAPACKAGE_DIR / datapackage_name / "datapackage.json",
    )
    es = EnergySystem.from_datapackage(datapackage_json, typemap=TYPEMAP)

    m = Model(es)
    m.add_constraints_from_datapackage(
        datapackage_json,
        constraint_type_map=CONSTRAINT_TYPE_MAP,
    )
    m.solve("cbc")

    es.params = processing.parameter_as_dict(es)
    return m.results()


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


def store_results_in_db(scenario_id: int, results: dict) -> None:
    """Store the results in the database."""


if __name__ == "__main__":
    results_ = simulate_datapackage(settings.OEMOF_SCENARIO)
    if settings.OEMOF_WRITE_RESULTS:
        store_results_in_folder(settings.OEMOF_SCENARIO, results_)
