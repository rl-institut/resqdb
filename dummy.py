import random
from typing import List

from sqlalchemy.orm import sessionmaker

try:
    from faker import Faker
except ImportError:  # graceful fallback if Faker isn't installed
    Faker = None  # type: ignore

from geoalchemy2.elements import WKTElement

from models import (
    ENGINE,
    setup_db,
    Weather,
    Climate,
    Scenario,
    Sensitivity,
    Cluster,
    Flow,
    Result,
)


def _fake() -> "Faker":
    if Faker is None:
        raise RuntimeError(
            "Faker is not installed. Please install it first, e.g. `pip install Faker` "
            "or add it to your project dependencies."
        )
    return Faker()


def _random_polygon_wkt() -> str:
    """Create a small square polygon around a random lon/lat in WKT. SRID=4326."""
    # Keep coordinates within valid ranges
    center_lon = random.uniform(-179.0, 179.0)
    center_lat = random.uniform(-85.0, 85.0)

    # small square, ~0.1 degree side
    d = random.uniform(0.05, 0.3)
    lons = [center_lon - d, center_lon + d, center_lon + d, center_lon - d, center_lon - d]
    lats = [center_lat - d, center_lat - d, center_lat + d, center_lat + d, center_lat - d]

    coords = ", ".join(f"{lon} {lat}" for lon, lat in zip(lons, lats))
    return f"POLYGON(({coords}))"


def _random_timeseries(n: int) -> List[float]:
    # Simple random float series; values between -100 and 100
    return [round(random.uniform(-100.0, 100.0), 3) for _ in range(n)]


def generate_dummy_data(
    n_weather: int = 3,
    n_climate: int = 3,
    n_scenarios: int = 5,
    n_clusters: int = 4,
    n_flows: int = 20,
    n_results: int = 10,
    n_sensitivities: int = 15,
    flow_series_len: int = 24,
) -> None:
    """
    Populate the database with randomly generated data for all tables defined in models.py.

    Notes:
    - This seeder honors the ForeignKey constraints currently defined in models.py:
      - Scenario.weather_id and Scenario.climate_id are set to valid Weather/Climate rows.
      - Flow.scenario_id and Result.scenario_id are set; cluster_id is optionally set.
      - Sensitivity.scenario_id is set.
    """

    fake = _fake()

    Session = sessionmaker(bind=ENGINE)
    session = Session()
    try:
        # Weather
        weathers = []
        for _ in range(n_weather):
            w = Weather(name=fake.word(), description=fake.sentence(nb_words=6))
            session.add(w)
            weathers.append(w)

        # Flush to assign IDs for FKs downstream
        session.flush()

        # Climate
        climates = []
        for _ in range(n_climate):
            c = Climate(name=fake.word(), description=fake.sentence(nb_words=6))
            session.add(c)
            climates.append(c)

        session.flush()

        # Scenario with required FKs to Weather/Climate
        scenarios = []
        for _ in range(n_scenarios):
            s = Scenario(
                year=fake.random_int(min=1990, max=2050),
                weather_id=random.choice(weathers).id,
                climate_id=random.choice(climates).id,
            )
            session.add(s)
            scenarios.append(s)

        session.flush()

        # Cluster with simple polygons
        clusters = []
        for _ in range(n_clusters):
            wkt = _random_polygon_wkt()
            geom = WKTElement(wkt, srid=4326)
            cl = Cluster(name=fake.city(), geometry=geom)
            session.add(cl)
            clusters.append(cl)

        session.flush()

        # Flow with required scenario_id; cluster_id optional
        for _ in range(n_flows):
            f = Flow(
                scenario_id=random.choice(scenarios).id,
                from_node=fake.bothify(text="node_##??"),
                to_node=fake.bothify(text="node_##??"),
                timeseries=_random_timeseries(flow_series_len),
                cluster_id=(random.choice(clusters).id if clusters and random.random() < 0.6 else None),
            )
            session.add(f)

        # Result with required scenario_id; cluster_id optional
        for _ in range(n_results):
            r = Result(
                scenario_id=random.choice(scenarios).id,
                name=fake.word(),
                value=round(fake.pyfloat(left_digits=2, right_digits=3), 3),
                cluster_id=(random.choice(clusters).id if clusters and random.random() < 0.6 else None),
            )
            session.add(r)

        # Sensitivity requires scenario_id
        attrs = ["efficiency", "capacity", "cost", "emission", "availability"]
        for _ in range(n_sensitivities):
            s = Sensitivity(
                scenario_id=random.choice(scenarios).id,
                node=fake.bothify(text="node_##??"),
                attribute=random.choice(attrs),
                value=round(fake.pyfloat(left_digits=2, right_digits=3, positive=False), 3),
            )
            session.add(s)

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    generate_dummy_data()
