import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values, Json
from datetime import date
from decimal import Decimal
from typing import Iterator
import logging

from markets_core.domain.observation import Observation
from markets_core.errors import StorageError

logger = logging.getLogger(__name__)


class TimescaleWarehouse:
    """Store and query observations from TimescaleDB."""
    
    def __init__(self, dsn: str):
        """Initialize with database connection string."""
        self.dsn = dsn
    
    def _get_conn(self):
        """Get database connection."""
        return psycopg2.connect(self.dsn)
    
    def upsert_observations(self, observations: list[Observation]) -> None:
        """Upsert observations into warehouse."""
        if not observations:
            return
        
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            
            # Prepare data for bulk insert
            values = [
                (
                    obs.series_id,
                    obs.observation_date,
                    obs.as_of_date,
                    obs.value,
                    obs.ingested_at,
                    obs.source_revision,
                )
                for obs in observations
            ]
            
            # Upsert: insert on conflict do update
            insert_sql = """
                INSERT INTO observations.observations 
                (series_id, observation_date, as_of_date, value, ingested_at, source_revision)
                VALUES %s
                ON CONFLICT (series_id, observation_date, as_of_date) 
                DO UPDATE SET 
                    value = EXCLUDED.value,
                    ingested_at = EXCLUDED.ingested_at,
                    source_revision = EXCLUDED.source_revision
            """
            
            execute_values(cur, insert_sql, values)
            conn.commit()
            
            logger.info(
                "warehouse_upsert_completed",
                observations=len(observations),
            )
            
            cur.close()
            conn.close()
        except Exception as e:
            raise StorageError(
                f"Failed to upsert observations: {e}",
            ) from e
    
    def query_point_in_time(self, series_ids: list[str], as_of: date) -> dict:
        """Query point-in-time observations as of a date."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            
            query = """
                SELECT DISTINCT ON (series_id, observation_date)
                    series_id, observation_date, as_of_date, value
                FROM observations.observations
                WHERE as_of_date <= %s
                  AND series_id = ANY(%s)
                ORDER BY series_id, observation_date, as_of_date DESC
            """
            
            cur.execute(query, (as_of, series_ids))
            results = cur.fetchall()
            
            # Group by series
            data = {}
            for series_id, obs_date, as_of_date, value in results:
                if series_id not in data:
                    data[series_id] = []
                data[series_id].append({
                    "observation_date": obs_date,
                    "as_of_date": as_of_date,
                    "value": value,
                })
            
            cur.close()
            conn.close()
            
            return data
        except Exception as e:
            raise StorageError(
                f"Failed to query point-in-time observations: {e}",
            ) from e
    
    def upsert_zscores(self, zscores: list[dict]) -> None:
        """Upsert z-scores into warehouse."""
        if not zscores:
            return
        
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            
            values = [
                (
                    z["series_id"],
                    z["observation_date"],
                    z["as_of_date"],
                    z["window_days"],
                    z["zscore"],
                )
                for z in zscores
            ]
            
            insert_sql = """
                INSERT INTO derived.zscores
                (series_id, observation_date, as_of_date, window_days, zscore)
                VALUES %s
                ON CONFLICT (series_id, observation_date, as_of_date, window_days)
                DO UPDATE SET zscore = EXCLUDED.zscore
            """
            
            execute_values(cur, insert_sql, values)
            conn.commit()
            
            logger.info(
                "zscores_upsert_completed",
                count=len(zscores),
            )
            
            cur.close()
            conn.close()
        except Exception as e:
            raise StorageError(
                f"Failed to upsert z-scores: {e}",
            ) from e
    
    def query_zscores(self, series_id: str, as_of: date, window_days: int = 63) -> dict:
        """Query z-scores for a series as of a date."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            
            query = """
                SELECT DISTINCT ON (observation_date)
                    observation_date, zscore
                FROM derived.zscores
                WHERE series_id = %s
                  AND as_of_date <= %s
                  AND window_days = %s
                ORDER BY observation_date, as_of_date DESC
            """
            
            cur.execute(query, (series_id, as_of, window_days))
            results = cur.fetchall()
            
            data = {}
            for obs_date, zscore in results:
                data[obs_date] = zscore
            
            cur.close()
            conn.close()
            
            return data
        except Exception as e:
            raise StorageError(
                f"Failed to query z-scores: {e}",
            ) from e
    
    def upsert_regime_states(self, states: list[dict]) -> None:
        """Upsert regime states (plural). Backward-compatible: new columns default if absent."""
        if not states:
            return

        try:
            conn = self._get_conn()
            cur = conn.cursor()

            values = [
                (
                    s["as_of_date"],
                    s["classifier_name"],
                    s["classifier_version"],
                    s["regime_name"],
                    s["confidence"],
                    s["trigger_variables"],
                    s["rationale"],
                    Json(s.get("rationale_detail", {})),
                    s.get("ontology_version", ""),
                )
                for s in states
            ]

            insert_sql = """
                INSERT INTO regimes.regime_states
                (as_of_date, classifier_name, classifier_version, regime_name,
                 confidence, trigger_variables, rationale, rationale_detail, ontology_version)
                VALUES %s
                ON CONFLICT (as_of_date, classifier_name, classifier_version)
                DO UPDATE SET
                    regime_name = EXCLUDED.regime_name,
                    confidence = EXCLUDED.confidence,
                    trigger_variables = EXCLUDED.trigger_variables,
                    rationale = EXCLUDED.rationale,
                    rationale_detail = EXCLUDED.rationale_detail,
                    ontology_version = EXCLUDED.ontology_version
            """

            execute_values(cur, insert_sql, values)
            conn.commit()

            logger.info(
                "regime_states_upsert_completed",
                count=len(states),
            )

            cur.close()
            conn.close()
        except Exception as e:
            raise StorageError(
                f"Failed to upsert regime states: {e}",
            ) from e

    def upsert_regime_state(self, row: dict) -> None:
        """Upsert a single regime state row including rationale_detail and ontology_version."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO regimes.regime_states
                (as_of_date, classifier_name, classifier_version, regime_name,
                 confidence, trigger_variables, rationale, rationale_detail, ontology_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (as_of_date, classifier_name, classifier_version)
                DO UPDATE SET
                    regime_name = EXCLUDED.regime_name,
                    confidence = EXCLUDED.confidence,
                    trigger_variables = EXCLUDED.trigger_variables,
                    rationale = EXCLUDED.rationale,
                    rationale_detail = EXCLUDED.rationale_detail,
                    ontology_version = EXCLUDED.ontology_version
                """,
                (
                    row["as_of_date"],
                    row["classifier_name"],
                    row["classifier_version"],
                    row["regime_name"],
                    row["confidence"],
                    row["trigger_variables"],
                    row.get("rationale"),
                    Json(row.get("rationale_detail", {})),
                    row.get("ontology_version", ""),
                ),
            )
            conn.commit()

            logger.info(
                "regime_state_upsert_completed",
                as_of_date=str(row["as_of_date"]),
                classifier=row["classifier_name"],
                regime=row["regime_name"],
            )

            cur.close()
            conn.close()
        except Exception as e:
            raise StorageError(
                f"Failed to upsert regime state: {e}",
            ) from e

    def read_observations_point_in_time(
        self,
        series_ids: list[str],
        as_of_date: date,
        start: date | None = None,
    ) -> list[dict]:
        """Return latest-known-as-of observations for the given series.

        Returns a list of dicts with keys: series_id, observation_date, as_of_date, value.
        """
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            if start is not None:
                query = """
                    SELECT DISTINCT ON (series_id, observation_date)
                        series_id, observation_date, as_of_date, value
                    FROM observations.observations
                    WHERE as_of_date <= %s
                      AND series_id = ANY(%s)
                      AND observation_date >= %s
                    ORDER BY series_id, observation_date, as_of_date DESC
                """
                cur.execute(query, (as_of_date, series_ids, start))
            else:
                query = """
                    SELECT DISTINCT ON (series_id, observation_date)
                        series_id, observation_date, as_of_date, value
                    FROM observations.observations
                    WHERE as_of_date <= %s
                      AND series_id = ANY(%s)
                    ORDER BY series_id, observation_date, as_of_date DESC
                """
                cur.execute(query, (as_of_date, series_ids))

            rows = cur.fetchall()
            cur.close()
            conn.close()

            return [
                {
                    "series_id": r[0],
                    "observation_date": r[1],
                    "as_of_date": r[2],
                    "value": r[3],
                }
                for r in rows
            ]
        except Exception as e:
            raise StorageError(
                f"Failed to read point-in-time observations: {e}",
            ) from e

    def upsert_yoy_changes(self, rows: list[dict]) -> None:
        """Upsert year-over-year change rows into derived.yoy_changes."""
        if not rows:
            return

        try:
            conn = self._get_conn()
            cur = conn.cursor()

            values = [
                (
                    r["series_id"],
                    r["observation_date"],
                    r["as_of_date"],
                    r["yoy_pct"],
                )
                for r in rows
            ]

            insert_sql = """
                INSERT INTO derived.yoy_changes
                (series_id, observation_date, as_of_date, yoy_pct)
                VALUES %s
                ON CONFLICT (series_id, observation_date, as_of_date)
                DO UPDATE SET yoy_pct = EXCLUDED.yoy_pct
            """

            execute_values(cur, insert_sql, values)
            conn.commit()

            logger.info(
                "yoy_changes_upsert_completed",
                count=len(rows),
            )

            cur.close()
            conn.close()
        except Exception as e:
            raise StorageError(
                f"Failed to upsert yoy changes: {e}",
            ) from e

    def upsert_trends(self, rows: list[dict]) -> None:
        """Upsert trend slope rows into derived.trends."""
        if not rows:
            return

        try:
            conn = self._get_conn()
            cur = conn.cursor()

            values = [
                (
                    r["series_id"],
                    r["observation_date"],
                    r["as_of_date"],
                    r["window_days"],
                    r["slope"],
                )
                for r in rows
            ]

            insert_sql = """
                INSERT INTO derived.trends
                (series_id, observation_date, as_of_date, window_days, slope)
                VALUES %s
                ON CONFLICT (series_id, observation_date, as_of_date, window_days)
                DO UPDATE SET slope = EXCLUDED.slope
            """

            execute_values(cur, insert_sql, values)
            conn.commit()

            logger.info(
                "trends_upsert_completed",
                count=len(rows),
            )

            cur.close()
            conn.close()
        except Exception as e:
            raise StorageError(
                f"Failed to upsert trends: {e}",
            ) from e

    def read_zscore_point_in_time(
        self,
        variable_series_id: str,
        window_days: int,
        as_of_date: date,
    ) -> Decimal | None:
        """Return the most recent z-score known as of as_of_date, or None if missing."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT zscore
                FROM derived.zscores
                WHERE series_id = %s
                  AND window_days = %s
                  AND as_of_date <= %s
                ORDER BY observation_date DESC, as_of_date DESC
                LIMIT 1
                """,
                (variable_series_id, window_days, as_of_date),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()

            return Decimal(str(row[0])) if row is not None and row[0] is not None else None
        except Exception as e:
            raise StorageError(
                f"Failed to read zscore point-in-time: {e}",
            ) from e

    def read_yoy_change_point_in_time(
        self,
        variable_series_id: str,
        as_of_date: date,
    ) -> Decimal | None:
        """Return the most recent yoy_pct known as of as_of_date, or None if missing."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT yoy_pct
                FROM derived.yoy_changes
                WHERE series_id = %s
                  AND as_of_date <= %s
                ORDER BY observation_date DESC, as_of_date DESC
                LIMIT 1
                """,
                (variable_series_id, as_of_date),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()

            return Decimal(str(row[0])) if row is not None and row[0] is not None else None
        except Exception as e:
            raise StorageError(
                f"Failed to read yoy change point-in-time: {e}",
            ) from e

    def read_trend_point_in_time(
        self,
        variable_series_id: str,
        window_days: int,
        as_of_date: date,
    ) -> Decimal | None:
        """Return the most recent slope known as of as_of_date, or None if missing."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT slope
                FROM derived.trends
                WHERE series_id = %s
                  AND window_days = %s
                  AND as_of_date <= %s
                ORDER BY observation_date DESC, as_of_date DESC
                LIMIT 1
                """,
                (variable_series_id, window_days, as_of_date),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()

            return Decimal(str(row[0])) if row is not None and row[0] is not None else None
        except Exception as e:
            raise StorageError(
                f"Failed to read trend point-in-time: {e}",
            ) from e

    def read_raw_value_point_in_time(
        self,
        variable_series_id: str,
        as_of_date: date,
    ) -> Decimal | None:
        """Return the most recent observation value known as of as_of_date, or None."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT DISTINCT ON (observation_date)
                    value
                FROM observations.observations
                WHERE series_id = %s
                  AND as_of_date <= %s
                ORDER BY observation_date DESC, as_of_date DESC
                LIMIT 1
                """,
                (variable_series_id, as_of_date),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()

            return Decimal(str(row[0])) if row is not None and row[0] is not None else None
        except Exception as e:
            raise StorageError(
                f"Failed to read raw value point-in-time: {e}",
            ) from e

    def read_ontology_variables(self) -> list[dict]:
        """Return all variables from ontology.variables (id, name, primary_series, tier)."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cur.execute(
                "SELECT id, name, primary_series, tier FROM ontology.variables ORDER BY id"
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()

            return [
                {
                    "id": r[0],
                    "name": r[1],
                    "primary_series": r[2],
                    "tier": r[3],
                }
                for r in rows
            ]
        except Exception as e:
            raise StorageError(
                f"Failed to read ontology variables: {e}",
            ) from e

    def read_ontology_regimes(self) -> list[dict]:
        """Return all regimes from ontology.regimes (id, name, display_name, description, tier)."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cur.execute(
                "SELECT id, name, display_name, description, tier FROM ontology.regimes ORDER BY id"
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()

            return [
                {
                    "id": r[0],
                    "name": r[1],
                    "display_name": r[2],
                    "description": r[3],
                    "tier": r[4],
                }
                for r in rows
            ]
        except Exception as e:
            raise StorageError(
                f"Failed to read ontology regimes: {e}",
            ) from e

    def read_ontology_regime_triggers(self) -> list[dict]:
        """Return all regime triggers (id, regime_id, variable_id, condition, weight, description).

        The condition column is jsonb; psycopg2 returns it as a Python dict automatically.
        """
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT id, regime_id, variable_id, condition, weight, description
                FROM ontology.regime_triggers
                ORDER BY id
                """
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()

            return [
                {
                    "id": r[0],
                    "regime_id": r[1],
                    "variable_id": r[2],
                    "condition": r[3],
                    "weight": r[4],
                    "description": r[5],
                }
                for r in rows
            ]
        except Exception as e:
            raise StorageError(
                f"Failed to read ontology regime triggers: {e}",
            ) from e

    def read_ontology_version(self) -> str:
        """Return the current Alembic migration version."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cur.execute("SELECT version_num FROM alembic_version LIMIT 1")
            row = cur.fetchone()
            cur.close()
            conn.close()

            return row[0] if row is not None else ""
        except Exception as e:
            raise StorageError(
                f"Failed to read ontology version: {e}",
            ) from e

    def execute_sql(self, sql: str, params: tuple | None = None) -> None:
        """Execute arbitrary SQL. Escape hatch for test cleanup and ad-hoc ops."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            raise StorageError(
                f"Failed to execute SQL: {e}",
            ) from e

    def query_regime_triggers(self) -> list[dict]:
        """Query regime triggers from ontology."""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            
            query = """
                SELECT rt.regime_id, rt.variable_id, r.name as regime_name, v.name as variable_name,
                       rt.condition, rt.weight
                FROM ontology.regime_triggers rt
                JOIN ontology.regimes r ON rt.regime_id = r.id
                JOIN ontology.variables v ON rt.variable_id = v.id
            """
            
            cur.execute(query)
            results = cur.fetchall()
            
            data = []
            for regime_id, var_id, regime_name, var_name, condition, weight in results:
                data.append({
                    "regime_id": regime_id,
                    "variable_id": var_id,
                    "regime_name": regime_name,
                    "variable_name": var_name,
                    "condition": condition,
                    "weight": weight,
                })
            
            cur.close()
            conn.close()
            
            return data
        except Exception as e:
            raise StorageError(
                f"Failed to query regime triggers: {e}",
            ) from e