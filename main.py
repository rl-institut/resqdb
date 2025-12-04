
import scenarios
import simulation
import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="oemof.solph")


def run():
    oemof_results = simulation.simulate_datapackge("investment")
    scenario_id = scenarios.create_scenario(2050, "hot", "rainy")
    scenarios.store_scenario_results(scenario_id, oemof_results)


if __name__ == "__main__":
    scenarios.delete_all_scenarios()
    run()