"""
NBA Betting - Predictions Module

Generates game predictions using trained AutoGluon models.
"""

from src.predictions.generator import (
    Predictions,
    main_predictions,
    on_demand_predictions,
    backtest_predictions,
)

__all__ = [
    "Predictions",
    "main_predictions",
    "on_demand_predictions",
    "backtest_predictions",
]
