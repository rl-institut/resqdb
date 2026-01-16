"""Module to run oemof simulations and store results."""

from oemof.solph import EnergySystem, Model, processing
from oemof.tabular import datapackage  # noqa: F401
from oemof.tabular.constraint_facades import CONSTRAINT_TYPE_MAP
from oemof.tabular.facades import TYPEMAP

import hooks
import settings


def simulate_datapackage(
    datapackage_name: str,
    parameters: dict | None = None,
) -> dict:
    """
    Simulate a data package and return results.

    This function loads JSON definition, builds an energy system,
    adds constraints, and solves it using the specified solver. Returns the results
    of the simulation as a dictionary.

    Args:
        datapackage_name (str): The name of the data package folder, which contains
            a `datapackage.json` file. This file defines the energy system structure,
            constraints, and other necessary components for the simulation.
        parameters (dict or None): A dictionary containing parameter values to adapt ES components; defaults to None

    Returns:
        dict: The simulation results, containing outputs such as optimized values
            or any other data produced after solving the system.

    """
    datapackage_json = str(
        settings.DATAPACKAGE_DIR / datapackage_name / "datapackage.json",
    )
    es = EnergySystem.from_datapackage(datapackage_json, typemap=TYPEMAP)

    # Adapt ES from parameters
    parameters = parameters or {}
    parameters = hooks.set_up_capacities(parameters)
    es = hooks.adapt_energysystem(es, parameters)

    m = Model(es)
    m.add_constraints_from_datapackage(
        datapackage_json,
        constraint_type_map=CONSTRAINT_TYPE_MAP,
    )
    m.solve("cbc")

    es.params = processing.parameter_as_dict(es)
    return m.results()


if __name__ == "__main__":
    results_ = simulate_datapackage(settings.OEMOF_SCENARIO)
