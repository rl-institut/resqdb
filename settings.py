"""Holds settings for the project."""

import json
import os
import pathlib
import warnings

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import Engine, create_engine

warnings.filterwarnings("ignore", category=FutureWarning, module="oemof.solph")

env_file = os.environ.get("ENV_FILE", ".env")
load_dotenv(env_file)

DEBUG = os.getenv("DEBUG", "False") == "True"

ROOT_DIR = pathlib.Path(__file__).parent
CONFIG_DIR = ROOT_DIR / "config"
DATAPACKAGE_DIR = ROOT_DIR / "datapackages"
RESULTS_DIR = ROOT_DIR / "results"
GEOPACKAGES_DIR = ROOT_DIR / "geopackages"
VIEWS_DIR = ROOT_DIR / "views"

# --- Database Configuration ---
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_SCHEMA = os.environ.get("DB_SCHEMA", "resqenergy")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def connect_to_db(url: str) -> Engine:
    """Connect to the PostgreSQL database."""
    logger.info(f"Connecting to database: {url}")
    return create_engine(
        DATABASE_URL,
        connect_args={"options": f"-c search_path={DB_SCHEMA},public"},
    )


ENGINE = connect_to_db(DATABASE_URL)

# --- OEMOF Settings ---
OEMOF_WRITE_RESULTS = os.getenv("OEMOF_WRITE_RESULTS", "False") == "True"
OEMOF_SCENARIO = os.getenv("OEMOF_SCENARIO", "dispatch")
OEMOF_OVERWRITE_RESULTS = os.getenv("OEMOF_OVERWRITE_RESULTS", "False") == "True"

# --- Cluster Mapping ---
CLUSTER_COMPONENT_FILE = CONFIG_DIR / "clusters.json"
CLUSTER_GEOPACKAGE = GEOPACKAGES_DIR / "clusters.gpkg"


def _create_component_to_cluster_mapping(clusters_data: dict) -> dict:
    """
    Create a mapping from components to their corresponding clusters.

    This function processes a dictionary where keys represent cluster identifiers and
    values are lists of components belonging to each cluster. It generates a reverse
    mapping from components to their associated cluster. If a component is found in
    multiple clusters, the function raises an error to prevent ambiguity.

    Args:
        clusters_data (dict): A dictionary mapping cluster identifiers to lists of components.

    Returns:
        dict: A dictionary mapping components to their corresponding cluster identifiers.

    Raises:
        ValueError: If a component is present in more than one cluster.

    """
    component_mapping = {}
    for cluster, components in clusters_data.items():
        for component in components:
            if component in component_mapping:
                raise ValueError(
                    f"Component {component} found in multiple clusters: "
                    f"{component_mapping[component]} and {cluster}",
                )
            component_mapping[component] = cluster
    return component_mapping


with CLUSTER_COMPONENT_FILE.open("r", encoding="utf-8") as json_file:
    CLUSTERS = json.load(json_file)

COMPONENT_CLUSTERS = _create_component_to_cluster_mapping(CLUSTERS)

# --- Labels and categories Mapping ---
with (CONFIG_DIR / "labels.json").open("r", encoding="utf-8") as json_file:
    labels_raw = json.load(json_file)
LABELS: dict[tuple[str, str], str] = {
    tuple(key.split("|")): value for key, value in labels_raw.items()
}

with (CONFIG_DIR / "categories.json").open("r", encoding="utf-8") as json_file:
    categories_raw = json.load(json_file)
CATEGORIES: dict[tuple[str, str], str] = {
    tuple(key.split("|")): str(value) for key, value in categories_raw.items()
}
