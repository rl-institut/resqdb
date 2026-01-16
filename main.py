"""Main project script."""

import json

import export
import scenarios
import simulation
import settings
import views


def run() -> None:
    """Run the simulation and store results in the database."""
    with (settings.CONFIG_DIR / "capacities.json").open("r", encoding="utf-8") as f:
        capacities = json.load(f)
    oemof_results = simulation.simulate_datapackage("scenario_es", capacities)
    scenario_id = scenarios.create_scenario(2050, "hot", "rainy")
    export.store_scenario_results(scenario_id, oemof_results)
    views.create_all_views(recreate=True)


if __name__ == "__main__":
    scenarios.delete_all_scenarios()
    run()
