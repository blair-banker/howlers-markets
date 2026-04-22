import psycopg2
from psycopg2.extras import execute_values
from datetime import date
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
        """Upsert regime states."""
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
                )
                for s in states
            ]
            
            insert_sql = """
                INSERT INTO regimes.regime_states
                (as_of_date, classifier_name, classifier_version, regime_name, confidence, trigger_variables, rationale)
                VALUES %s
                ON CONFLICT (as_of_date, classifier_name, classifier_version)
                DO UPDATE SET 
                    regime_name = EXCLUDED.regime_name,
                    confidence = EXCLUDED.confidence,
                    trigger_variables = EXCLUDED.trigger_variables,
                    rationale = EXCLUDED.rationale
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