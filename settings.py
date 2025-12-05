"""Holds settings for the project."""

import os
import pathlib
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

CLUSTER_COMPONENT_FILE = ROOT_DIR / "clusters.json"
CLUSTER_GEOPACKAGE = GEOPACKAGES_DIR / "clusters.gpkg"

OEMOF_WRITE_RESULTS = os.getenv("OEMOF_WRITE_RESULTS", "False") == "True"
OEMOF_SCENARIO = os.getenv("OEMOF_SCENARIO", "dispatch")
OEMOF_OVERWRITE_RESULTS = os.getenv("OEMOF_OVERWRITE_RESULTS", "False") == "True"
