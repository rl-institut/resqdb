"""
Init models

Revision ID: 336b3d67aeae
Revises:
Create Date: 2026-02-10 11:15:07.530361

"""

from typing import Union
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision: str = "336b3d67aeae"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "category",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("from_node", sa.String(), nullable=False),
        sa.Column("to_node", sa.String(), nullable=True),
        sa.Column("category", sa.String(), nullable=False),
        sa.Column("carrier", sa.String(), nullable=False),
        sa.Column("is_renewable", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("from_node", "to_node"),
        schema="public",
    )
    op.create_table(
        "climate",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
        schema="public",
    )
    op.create_table(
        "cluster",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                geometry_type="POLYGON",
                srid=4326,
                dimension=2,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
        schema="public",
    )
    # op.create_index('idx_cluster_geometry', 'cluster', ['geometry'], unique=False, schema='public', postgresql_using='gist')
    op.create_table(
        "label",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("component", sa.String(), nullable=False),
        sa.Column("is_bus", sa.Boolean(), nullable=False),
        sa.Column("label", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("component"),
        schema="public",
    )
    op.create_table(
        "period",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("reference_year", sa.Integer(), nullable=True),
        sa.Column("period_start", sa.Integer(), nullable=True),
        sa.Column("period_end", sa.Integer(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
        schema="public",
    )
    op.create_table(
        "sensitivity",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("node", sa.String(), nullable=True),
        sa.Column("attribute", sa.String(), nullable=True),
        sa.Column("value", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )
    op.create_table(
        "weather",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
        schema="public",
    )
    op.create_table(
        "cluster_component",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("from_node", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["cluster_id"],
            ["public.cluster.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )
    op.create_table(
        "scenario",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("period_id", sa.Integer(), nullable=False),
        sa.Column("weather_id", sa.Integer(), nullable=False),
        sa.Column("climate_id", sa.Integer(), nullable=False),
        sa.Column("sensitivity_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["climate_id"],
            ["public.climate.id"],
        ),
        sa.ForeignKeyConstraint(
            ["period_id"],
            ["public.period.id"],
        ),
        sa.ForeignKeyConstraint(
            ["sensitivity_id"],
            ["public.sensitivity.id"],
        ),
        sa.ForeignKeyConstraint(
            ["weather_id"],
            ["public.weather.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
        schema="public",
    )
    op.create_index(
        "scenario_with_sensitivity",
        "scenario",
        ["period_id", "weather_id", "climate_id", "sensitivity_id"],
        unique=True,
        schema="public",
        postgresql_where=sa.text("sensitivity_id IS NOT NULL"),
    )
    op.create_index(
        "scenario_without_sensitivity",
        "scenario",
        ["period_id", "weather_id", "climate_id"],
        unique=True,
        schema="public",
        postgresql_where=sa.text("sensitivity_id IS NULL"),
    )
    op.create_table(
        "scalar",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("is_exogenous", sa.Boolean(), nullable=False),
        sa.Column("from_node", sa.String(), nullable=True),
        sa.Column("to_node", sa.String(), nullable=True),
        sa.Column("attribute", sa.String(), nullable=True),
        sa.Column("value", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["public.scenario.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )
    op.create_table(
        "sequence",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("scenario_id", sa.Integer(), nullable=False),
        sa.Column("is_exogenous", sa.Boolean(), nullable=False),
        sa.Column("from_node", sa.String(), nullable=True),
        sa.Column("to_node", sa.String(), nullable=True),
        sa.Column("attribute", sa.String(), nullable=True),
        sa.Column("total_energy", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["scenario_id"],
            ["public.scenario.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )
    op.create_index(
        "sequence_unique",
        "sequence",
        ["scenario_id", "from_node", "to_node", "attribute"],
        unique=True,
        schema="public",
    )
    op.create_table(
        "timeseries",
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("value", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["sequence_id"],
            ["public.sequence.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("sequence_id", "timestamp"),
        schema="public",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("timeseries", schema="public")
    op.drop_index("sequence_unique", table_name="sequence", schema="public")
    op.drop_table("sequence", schema="public")
    op.drop_table("scalar", schema="public")
    op.drop_index(
        "scenario_without_sensitivity",
        table_name="scenario",
        schema="public",
        postgresql_where=sa.text("sensitivity_id IS NULL"),
    )
    op.drop_index(
        "scenario_with_sensitivity",
        table_name="scenario",
        schema="public",
        postgresql_where=sa.text("sensitivity_id IS NOT NULL"),
    )
    op.drop_table("scenario", schema="public")
    op.drop_table("cluster_component", schema="public")
    op.drop_table("weather", schema="public")
    op.drop_table("sensitivity", schema="public")
    op.drop_table("period", schema="public")
    op.drop_table("label", schema="public")
    op.drop_index(
        "idx_cluster_geometry",
        table_name="cluster",
        schema="public",
        postgresql_using="gist",
    )
    op.drop_table("cluster", schema="public")
    op.drop_table("climate", schema="public")
    op.drop_table("category", schema="public")
