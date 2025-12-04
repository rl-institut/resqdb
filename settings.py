"""Holds settings for the project."""

import os
import pathlib
import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="oemof.solph")

DEBUG = os.getenv("DEBUG", "False") == "True"

ROOT_DIR = pathlib.Path(__file__).parent
DATAPACKAGE_DIR = ROOT_DIR / "datapackages"
RESULTS_DIR = ROOT_DIR / "results"
GEOPACKAGES_DIR = ROOT_DIR / "geopackages"

OEMOF_WRITE_RESULTS = os.getenv("OEMOF_WRITE_RESULTS", "False") == "True"
OEMOF_SCENARIO = os.getenv("OEMOF_SCENARIO", "dispatch")
OEMOF_OVERWRITE_RESULTS = os.getenv("OEMOF_OVERWRITE_RESULTS", "False") == "True"

CLUSTER_COMPONENT_FILE = ROOT_DIR / "clusters.json"
