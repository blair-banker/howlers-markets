"""Stage 4 classifier schema additions

Revision ID: 002
Revises: 001
Create Date: 2026-04-22 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # derived.yoy_changes
    op.create_table(
        "yoy_changes",
        sa.Column("series_id", sa.Text(), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("yoy_pct", sa.Numeric(10, 6), nullable=True),
        sa.Column(
            "computed_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("series_id", "observation_date", "as_of_date"),
        schema="derived",
    )
    op.execute("SELECT create_hypertable('derived.yoy_changes', 'observation_date')")

    # derived.trends
    op.create_table(
        "trends",
        sa.Column("series_id", sa.Text(), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("window_days", sa.Integer(), nullable=False),
        sa.Column("slope", sa.Numeric(10, 6), nullable=True),
        sa.Column(
            "computed_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "series_id", "observation_date", "as_of_date", "window_days"
        ),
        schema="derived",
    )
    op.execute("SELECT create_hypertable('derived.trends', 'observation_date')")

    # ontology.regime_triggers.condition: text -> jsonb
    # Existing rows contain DSL strings (e.g. "zscore_3m > 2.0"); wrap them as
    # JSON string values via to_jsonb() so the cast succeeds on existing data.
    op.execute(
        "ALTER TABLE ontology.regime_triggers "
        "ALTER COLUMN condition TYPE jsonb USING to_jsonb(condition)"
    )

    # regimes.regime_states: add rationale_detail, ontology_version
    op.add_column(
        "regime_states",
        sa.Column(
            "rationale_detail",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        schema="regimes",
    )
    op.add_column(
        "regime_states",
        sa.Column(
            "ontology_version",
            sa.Text(),
            nullable=False,
            server_default=sa.text("''"),
        ),
        schema="regimes",
    )


def downgrade() -> None:
    op.drop_column("regime_states", "ontology_version", schema="regimes")
    op.drop_column("regime_states", "rationale_detail", schema="regimes")
    op.execute(
        "ALTER TABLE ontology.regime_triggers "
        "ALTER COLUMN condition TYPE text USING condition::text"
    )
    op.drop_table("trends", schema="derived")
    op.drop_table("yoy_changes", schema="derived")
