"""Create materialized views from SQL files."""

from collections.abc import Iterable

from sqlalchemy.orm import Session
from sqlalchemy import text

from settings import VIEWS_DIR, ENGINE


def get_views() -> Iterable[tuple[str, str]]:
    """Get views from SQL files in the views directory."""
    for view in VIEWS_DIR.iterdir():
        if view.suffix != ".sql":
            continue
        with view.open("r", encoding="utf-8") as view_file:
            query = view_file.read()
            query = query.strip(";")
            yield view.stem, query


def create_view(view_name: str, query: str, *, recreate: bool = False) -> None:
    """Create materialized view from SQL query."""
    with Session(ENGINE) as session:
        if recreate:
            session.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};"))
        session.execute(
            text(f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS ({query});"),
        )
        session.commit()


def create_all_views(*, recreate: bool = False) -> None:
    """Create all materialized views from SQL files in the views directory."""
    for view_name, query in get_views():
        create_view(view_name, query, recreate=recreate)


if __name__ == "__main__":
    create_all_views(recreate=True)
