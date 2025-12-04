"""Main project script."""

import scenarios
import simulation


def run() -> None:
    """Run the simulation and store results in the database."""
    oemof_results = simulation.simulate_datapackage("investment")
    scenario_id = scenarios.create_scenario(2050, "hot", "rainy")
    scenarios.store_scenario_results(scenario_id, oemof_results)


if __name__ == "__main__":
    scenarios.delete_all_scenarios()
    run()
