"""Main project script."""

import argparse

import export
import models
import scenarios
import simulation
import views


def run() -> None:
    """Run the simulation and store results in the database."""
    for file in scenarios.get_scenarios_in_folder():
        scenario_config = scenarios.load_scenario_settings_from_file(file)
        scenario_id, created = scenarios.create_scenario(**scenario_config.scenario)
        if created:
            oemof_results = simulation.simulate_datapackage(
                scenario_config.datapackage,
                scenario_config.capacities,
            )
            export.store_scenario_results(scenario_id, oemof_results)
    views.create_all_views(recreate=True)


def main() -> None:
    """
    Parse command-line arguments to execute database setup or teardown operations.

    This function defines a command-line interface (CLI) for managing a database
    through two primary commands: `setup` and `delete`. The `setup` command prepares
    the database configuration and initialization, while the `delete` command handles
    the teardown and removal of associated resources.

    Raises:
        SystemExit: Raised implicitly by `argparse` when invalid arguments are
            provided or if the help message is displayed.

    """
    parser = argparse.ArgumentParser(
        prog="resq",
        description="Commands for resqdb",
    )
    parser.set_defaults(func=run)

    subparsers = parser.add_subparsers()

    # DB setup command
    setup_parser = subparsers.add_parser("setup")
    setup_parser.set_defaults(func=models.setup_db)

    # DB delete command
    delete_parser = subparsers.add_parser("delete")
    delete_parser.set_defaults(func=models.teardown_db)

    args = parser.parse_args()
    args.func()


if __name__ == "__main__":
    main()
