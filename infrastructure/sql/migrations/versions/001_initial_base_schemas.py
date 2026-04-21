"""Initial base schemas

Revision ID: 001
Revises:
Create Date: 2026-04-21 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create schemas
    op.execute("CREATE SCHEMA IF NOT EXISTS observations")
    op.execute("CREATE SCHEMA IF NOT EXISTS derived")
    op.execute("CREATE SCHEMA IF NOT EXISTS ontology")
    op.execute("CREATE SCHEMA IF NOT EXISTS regimes")

    # observations.observations hypertable
    op.create_table(
        'observations',
        sa.Column('series_id', sa.Text(), nullable=False),
        sa.Column('observation_date', sa.Date(), nullable=False),
        sa.Column('as_of_date', sa.Date(), nullable=False),
        sa.Column('value', sa.Numeric(18, 8), nullable=True),
        sa.Column('ingested_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('source_revision', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('series_id', 'observation_date', 'as_of_date'),
        schema='observations'
    )
    op.execute("SELECT create_hypertable('observations.observations', 'observation_date', chunk_time_interval => INTERVAL '1 year')")
    op.create_index('observations_latest_as_of_idx', 'observations', ['series_id', 'observation_date', sa.text('as_of_date DESC')], schema='observations')

    # ontology.variables
    op.create_table(
        'variables',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.Text(), nullable=False, unique=True),
        sa.Column('display_name', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('column_id', sa.Integer(), nullable=True),  # Simplified, no FK for now
        sa.Column('depth_row_id', sa.Integer(), nullable=True),
        sa.Column('tier', sa.Integer(), nullable=False),
        sa.Column('primary_series', sa.Text(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        schema='ontology'
    )

    # ontology.regimes
    op.create_table(
        'regimes',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.Text(), nullable=False, unique=True),
        sa.Column('display_name', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=False),
        sa.Column('tier', sa.Integer(), nullable=False),
        schema='ontology'
    )

    # ontology.regime_triggers
    op.create_table(
        'regime_triggers',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('regime_id', sa.Integer(), nullable=False),
        sa.Column('variable_id', sa.Integer(), nullable=False),
        sa.Column('condition', sa.Text(), nullable=False),
        sa.Column('weight', sa.Numeric(4, 3), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        schema='ontology'
    )

    # derived.zscores
    op.create_table(
        'zscores',
        sa.Column('series_id', sa.Text(), nullable=False),
        sa.Column('observation_date', sa.Date(), nullable=False),
        sa.Column('as_of_date', sa.Date(), nullable=False),
        sa.Column('window_days', sa.Integer(), nullable=False),
        sa.Column('zscore', sa.Numeric(10, 4), nullable=True),
        sa.Column('computed_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('series_id', 'observation_date', 'as_of_date', 'window_days'),
        schema='derived'
    )
    op.execute("SELECT create_hypertable('derived.zscores', 'observation_date')")

    # regimes.regime_states
    op.create_table(
        'regime_states',
        sa.Column('as_of_date', sa.Date(), nullable=False),
        sa.Column('classifier_name', sa.Text(), nullable=False),
        sa.Column('classifier_version', sa.Text(), nullable=False),
        sa.Column('regime_name', sa.Text(), nullable=False),
        sa.Column('confidence', sa.Numeric(4, 3), nullable=False),
        sa.Column('trigger_variables', postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column('rationale', sa.Text(), nullable=True),
        sa.Column('computed_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('as_of_date', 'classifier_name', 'classifier_version'),
        schema='regimes'
    )
    op.execute("SELECT create_hypertable('regimes.regime_states', 'as_of_date')")


def downgrade() -> None:
    op.drop_table('regime_states', schema='regimes')
    op.drop_table('zscores', schema='derived')
    op.drop_table('regime_triggers', schema='ontology')
    op.drop_table('regimes', schema='ontology')
    op.drop_table('variables', schema='ontology')
    op.drop_table('observations', schema='observations')
    op.execute("DROP SCHEMA IF EXISTS regimes")
    op.execute("DROP SCHEMA IF EXISTS derived")
    op.execute("DROP SCHEMA IF EXISTS ontology")
    op.execute("DROP SCHEMA IF EXISTS observations")