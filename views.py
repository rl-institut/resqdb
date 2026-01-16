"""Create materialized views from SQL files."""

from collections.abc import Iterable

from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session

from settings import ENGINE, VIEWS_DIR


def get_views() -> Iterable[tuple[str, str]]:
    """
    Retrieve SQL views from a directory.

    Iterates through all files in the VIEWS directory, filters files with the .sql
    extension, reads their content, removes trailing semicolons, and retrieves their
    stem and query content as a tuple.

    Yields:
        Iterable[tuple[str, str]]: An iterable of tuples where the first element is the
        stem of the SQL file (filename without extension) and the second element is the
        SQL query content.

    """
    for view in VIEWS_DIR.iterdir():
        if view.suffix != ".sql":
            continue
        with view.open("r", encoding="utf-8") as view_file:
            query = view_file.read()
            query = query.replace(";", "")
            yield view.stem, query


def create_view(view_name: str, query: str, *, recreate: bool = False) -> None:
    """
    Create or recreate a materialized view in the database with the specified query.

    This function handles the creation of a materialized view in the database. If the
    `recreate` flag is set to True, it will first drop the materialized view if it
    exists before creating it again. Otherwise, it will ensure that the view exists
    but will not overwrite it if it already exists.

    Args:
        view_name (str): The name of the materialized view to be created or recreated.
        query (str): The SQL query used to define the view.
        recreate (bool, optional): When set to True, drops and recreates the view even
            if it already exists. Defaults to False.

    """
    with Session(ENGINE) as session:
        if recreate:
            session.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};"))
        session.execute(
            text(f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS ({query});"),
        )
        session.commit()
        logger.info(f"Created materialized view '{view_name}'.")


def create_all_views(*, recreate: bool = False) -> None:
    """
    Create all materialized views from SQL files in the views directory.

    Args:
        recreate (bool, optional): When set to True, drops and recreates all views even
            if it already exists. Defaults to False.

    """
    for view_name, query in get_views():
        create_view(view_name, query, recreate=recreate)


if __name__ == "__main__":
    create_all_views(recreate=True)
