"""
Forecaster package — UltimateForecaster integration for Makronóm MCP.

Adapted from Nexonomics forecasting system.
Ensemble: SAJÁT (Phillips Curve + Okun's Law) + IMF WEO + OECD CLI.
Supports 52 countries, GDP/inflation/unemployment, quarterly + monthly.
"""

from .ultimate_forecaster import UltimateForecaster

__all__ = ["UltimateForecaster"]
