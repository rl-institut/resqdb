"""Holds settings for the project."""

import os
import pathlib
import json
import warnings
from dotenv import load_dotenv
from sqlalchemy import create_engine

warnings.filterwarnings("ignore", category=FutureWarning, module="oemof.solph")

env_file = os.environ.get("ENV_FILE", ".env")
load_dotenv(env_file)

DEBUG = os.getenv("DEBUG", "False") == "True"

ROOT_DIR = pathlib.Path(__file__).parent
DATAPACKAGE_DIR = ROOT_DIR / "datapackages"
RESULTS_DIR = ROOT_DIR / "results"
GEOPACKAGES_DIR = ROOT_DIR / "geopackages"
VIEWS_DIR = ROOT_DIR / "views"

DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_SCHEMA = os.environ.get("DB_SCHEMA", "resqenergy")
ENGINE = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    connect_args={"options": "-c search_path=resqenergy,public"},
)

OEMOF_WRITE_RESULTS = os.getenv("OEMOF_WRITE_RESULTS", "False") == "True"
OEMOF_SCENARIO = os.getenv("OEMOF_SCENARIO", "dispatch")
OEMOF_OVERWRITE_RESULTS = os.getenv("OEMOF_OVERWRITE_RESULTS", "False") == "True"

# CLUSTER

CLUSTER_COMPONENT_FILE = ROOT_DIR / "clusters.json"
CLUSTER_GEOPACKAGE = GEOPACKAGES_DIR / "clusters.gpkg"
with CLUSTER_COMPONENT_FILE.open("r", encoding="utf-8") as json_file:
    CLUSTERS = json.load(json_file)


def _create_component_to_cluster_mapping(clusters: dict) -> dict:
    """Create a mapping from components to their clusters."""
    component_mapping = {}
    for cluster, components in clusters.items():
        for component in components:
            if component in component_mapping:
                raise ValueError(
                    f"Component {component} found in multiple clusters: "
                    f"{component_mapping[component]} and {cluster}",
                )
            component_mapping[component] = cluster
    return component_mapping


COMPONENT_CLUSTERS = _create_component_to_cluster_mapping(CLUSTERS)
