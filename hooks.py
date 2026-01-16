"""Hooks to adapt the energy system on-the-fly."""

from loguru import logger
from oemof.solph import EnergySystem


def set_up_capacities(capacities: dict) -> dict:
    """
    Set up capacities for volatiles, potentials, and load demand amounts.

    Args:
        capacities: Dictionary containing component capacities

    Returns:
        dict: A dictionary of parameters for the oemof simulation.

    """
    electricity_factor = capacities["mobility"] / 100
    mobility_demand = electricity_factor * 7652.15 * 1e-3  # Value from reenact
    parameters = {
        "wind": {"capacity": capacities["wind"], "expandable": False},
        "pv_ground": {"capacity": capacities["pv_ground"], "expandable": False},
        "pv_roof": {"capacity": capacities["pv_roof"], "expandable": False},
        "pv_agri": {"capacity": capacities["pv_agri"], "expandable": False},
        "pv_marsh": {"capacity": capacities["pv_marsh"], "expandable": False},
        "bio-new": {
            "amount": (
                capacities["other_biomass"] * 4.2  # Value from reenact
                + capacities["biomass_marsh"] * 4.03  # Value from reenact
            ),
        },
        "SB-backpressure": {
            "capacity": 0.64
            + capacities["other_biomass"] * 0.169485 / 1000  # Value from reenact
            + capacities["biomass_marsh"] * 0.16262489 / 1000,  # Value from reenact
        },
        "electrolyser": {"capacity": capacities["electrolyzer"], "expandable": False},
        "battery": {
            "capacity": capacities["battery"],
            "storage_capacity": capacities["battery"],
        },
        "electricity": {"amount": capacities["electricity"]},
        "heat": {"amount": capacities["heat"]},
        "mobility": {"amount": mobility_demand},
        "marsh": capacities["marsh"],
    }
    return parameters


# Copied from django-oemof v2.1.0
def adapt_energysystem(energysystem: EnergySystem, parameters: dict) -> EnergySystem:
    """
    Adapt parameters in ES.

    This allows loading standard ES and changing specific parameters after build.

    Parameters
    ----------
    energysystem: EnergySystem
        Components in Energysystem are changed by given parameters

    parameters: dict
        Parameters which shall be adapted in ES

    Returns
    -------
    energysystem: Energysystem with changed parameters

    """
    parameters = parameters or {}
    logger.info(f"Adapting parameters in ES using {parameters=}.")

    for node_name, attributes in parameters.items():
        if node_name == "flow":
            logger.warning(
                "This is deprecated. Flows are adapted using input_parameters and output_parameters instead.",
            )
            continue
        if node_name not in [n.label for n in energysystem.nodes]:
            log_msg = f"Cannot adapt component '{node_name}', as it cannot be found in energysystem."
            logger.warning(log_msg)
            continue
        if not isinstance(attributes, dict):
            log_msg = (
                f"Cannot adapt attributes for component '{node_name}', as there is no dictionary. Skipping it."
                """Try something like {"wind_onshore": {"capacity": 100}} instead."""
            )
            logger.warning(log_msg)
            continue

        node = next(n for n in energysystem.nodes if node_name == n.label)
        for attribute, value in attributes.items():
            if not hasattr(node, attribute):
                logger.warning(
                    f"Attribute '{attribute}' not found in component '{node_name}' in energysystem. "
                    "Adapting the attribute might have no effect.",
                )
            setattr(node, attribute, value)
        node.update()
    return energysystem
