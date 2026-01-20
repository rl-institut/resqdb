"""Main project script."""

import json

import export
import scenarios
import settings
import simulation
import views


def run() -> None:
    """Run the simulation and store results in the database."""
    with (settings.CONFIG_DIR / "capacities.json").open("r", encoding="utf-8") as f:
        capacities = json.load(f)
    scenario_id = scenarios.create_scenario("P1", "RCP8.5", "extreme2")
    oemof_results = simulation.simulate_datapackage("scenario_es", capacities)
    export.store_scenario_results(scenario_id, oemof_results)
    views.create_all_views(recreate=True)


if __name__ == "__main__":
    scenarios.delete_all_scenarios()
    run()
