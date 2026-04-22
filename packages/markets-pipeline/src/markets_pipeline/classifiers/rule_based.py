from datetime import date
from decimal import Decimal
import logging
import re

logger = logging.getLogger(__name__)


class RuleBasedClassifier:
    """Rule-based classifier using ontology trigger conditions."""
    
    def __init__(self, warehouse):
        """Initialize with warehouse connection."""
        self.warehouse = warehouse
        self.classifier_name = "rule_based"
        self.classifier_version = "v1"
    
    def classify(self, as_of: date) -> list[dict]:
        """Classify regime(s) as of a date."""
        try:
            # Query regime triggers
            triggers = self.warehouse.query_regime_triggers()
            
            if not triggers:
                logger.warning("no_regime_triggers_found")
                return []
            
            # Group triggers by regime
            regimes_by_name = {}
            for trigger in triggers:
                regime_name = trigger["regime_name"]
                if regime_name not in regimes_by_name:
                    regimes_by_name[regime_name] = []
                regimes_by_name[regime_name].append(trigger)
            
            # Evaluate each regime
            regime_states = []
            
            for regime_name, regime_triggers in regimes_by_name.items():
                # Evaluate all triggers for this regime
                trigger_results = []
                active_triggers = []
                
                for trigger in regime_triggers:
                    # For now, only support zscore triggers
                    if "zscore" not in trigger["condition"]:
                        continue
                    
                    variable_name = trigger["variable_name"]
                    condition = trigger["condition"]
                    weight = trigger["weight"]
                    
                    # Extract series_id from variable (for now, static mapping)
                    series_mapping = {
                        "us_10y_treasury": "fred:DGS10",
                    }
                    series_id = series_mapping.get(variable_name)
                    
                    if not series_id:
                        logger.warning(
                            "unknown_variable",
                            variable=variable_name,
                        )
                        continue
                    
                    # Query latest z-score
                    zscores = self.warehouse.query_zscores(series_id, as_of, 63)
                    
                    if not zscores:
                        continue
                    
                    # Get most recent z-score
                    latest_date = max(zscores.keys())
                    zscore = float(zscores[latest_date])
                    
                    # Evaluate condition
                    triggered = self._evaluate_condition(condition, zscore)
                    
                    trigger_results.append({
                        "variable": variable_name,
                        "condition": condition,
                        "zscore": zscore,
                        "triggered": triggered,
                        "weight": weight,
                    })
                    
                    if triggered:
                        active_triggers.append(variable_name)
                
                # Regime is active if ANY trigger is active (for now, simple OR logic)
                if active_triggers:
                    regime_states.append({
                        "as_of_date": as_of,
                        "classifier_name": self.classifier_name,
                        "classifier_version": self.classifier_version,
                        "regime_name": regime_name,
                        "confidence": Decimal("1.0"),
                        "trigger_variables": active_triggers,
                        "rationale": f"Triggered by: {', '.join(active_triggers)}",
                    })
            
            # If no regimes active, assign benign expansion
            if not regime_states:
                regime_states.append({
                    "as_of_date": as_of,
                    "classifier_name": self.classifier_name,
                    "classifier_version": self.classifier_version,
                    "regime_name": "benign_expansion",
                    "confidence": Decimal("1.0"),
                    "trigger_variables": [],
                    "rationale": "No stress regimes triggered",
                })
            
            logger.info(
                "classification_completed",
                as_of=as_of,
                regimes=len(regime_states),
            )
            
            return regime_states
        except Exception as e:
            logger.error(
                "classification_failed",
                as_of=as_of,
                error=str(e),
            )
            raise
    
    def _evaluate_condition(self, condition: str, zscore: float) -> bool:
        """Evaluate a simple condition string against a zscore value."""
        try:
            # Parse conditions like "zscore_3m > 2.0" or "zscore_3m <= 1.0"
            # Replace variable name with actual value
            condition = condition.replace("zscore_3m", str(zscore))
            condition = condition.replace("zscore", str(zscore))
            
            # Use eval (safe for simple numeric expressions)
            result = eval(condition)
            return bool(result)
        except Exception as e:
            logger.warning(
                "condition_eval_failed",
                condition=condition,
                zscore=zscore,
                error=str(e),
            )
            return False