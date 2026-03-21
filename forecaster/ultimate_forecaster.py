"""
ULTIMATE FORECASTER v1.0
========================
A világ legjobb ingyenes gazdasági előrejelző rendszere!

Kombinálja:
1. SAJÁT FORECASTER - Phillips Curve, Okun's Law, 3 scenarios (52 ország)
2. SUPER FORECASTER - Leading indicators ensemble (ifo, ZEW, Yield Curve, VIX)
3. IMF WEO - Hivatalos előrejelzések (DBnomics-on keresztül)
4. OECD CLI - Composite Leading Indicator
5. ECB - Eurozone monetáris adatok
6. Google Trends - Real-time sentiment (opcionális)
7. World Bank - Globális fejlettségi mutatók

EDGE: Negyedéves és havi forecasting MONOPOL!

Author: Claude (für Kommandant Tamás)
"""

import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass
from pathlib import Path

# Environment — FRED_API_KEY comes from Railway env vars
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

# =============================================================================
# LEADING INDICATORS CONFIGURATION
# =============================================================================

@dataclass
class LeadingIndicator:
    """Egy leading indicator metaadatai"""
    name: str
    source: str
    series_id: str
    lead_months: int  # Hány hónappal előzi meg a GDP-t
    weight: float     # Súly az ensemble-ben
    transform: str    # 'level', 'yoy', 'mom', 'diff'
    bullish_threshold: float  # Fölötte bullish
    bearish_threshold: float  # Alatta bearish

# Bővített leading indicator lista
LEADING_INDICATORS = {
    # === US INDICATORS (FRED) ===
    'us_consumer_sentiment': LeadingIndicator(
        name='Michigan Consumer Sentiment',
        source='FRED',
        series_id='UMCSENT',
        lead_months=2,
        weight=0.10,
        transform='level',
        bullish_threshold=80,
        bearish_threshold=60
    ),
    'us_yield_curve': LeadingIndicator(
        name='Yield Curve (10Y-2Y)',
        source='FRED',
        series_id='T10Y2Y',
        lead_months=12,  # Hosszú lead time - recession predictor!
        weight=0.15,
        transform='level',
        bullish_threshold=0.5,
        bearish_threshold=0
    ),
    'us_initial_claims': LeadingIndicator(
        name='Initial Jobless Claims (4wk avg)',
        source='FRED',
        series_id='IC4WSA',
        lead_months=1,
        weight=0.10,
        transform='level',
        bullish_threshold=250000,  # Inverse! Lower is better
        bearish_threshold=350000
    ),
    'us_building_permits': LeadingIndicator(
        name='Building Permits',
        source='FRED',
        series_id='PERMIT',
        lead_months=3,
        weight=0.08,
        transform='level',
        bullish_threshold=1500,
        bearish_threshold=1200
    ),
    'us_pmi_manufacturing': LeadingIndicator(
        name='ISM Manufacturing PMI',
        source='FRED',
        series_id='MANEMP',  # Manufacturing Employment as proxy
        lead_months=2,
        weight=0.10,
        transform='level',
        bullish_threshold=50,
        bearish_threshold=45
    ),
    'us_lei': LeadingIndicator(
        name='Philadelphia Fed Leading Index',
        source='FRED',
        series_id='USPHCI',  # Philadelphia Fed Coincident Index (as proxy)
        lead_months=6,
        weight=0.12,
        transform='level',
        bullish_threshold=1,
        bearish_threshold=-1
    ),

    # === GERMAN INDICATORS (via FRED) ===
    'de_ifo': LeadingIndicator(
        name='ifo Business Climate Germany',
        source='FRED',
        series_id='BSCICP02DEM460S',
        lead_months=2,
        weight=0.12,
        transform='level',
        bullish_threshold=100,
        bearish_threshold=95
    ),
    # Note: ZEW removed - using ifo + Eurostat confidence instead
    # The DELORSGPNOSTSAM series doesn't exist on FRED

    # === EUROZONE ===
    'eu_economic_sentiment': LeadingIndicator(
        name='EU Economic Sentiment Indicator',
        source='EUROSTAT',
        series_id='BS-ESI-I',
        lead_months=2,
        weight=0.08,
        transform='level',
        bullish_threshold=100,
        bearish_threshold=95
    ),
    'eu_consumer_confidence': LeadingIndicator(
        name='EU Consumer Confidence',
        source='EUROSTAT',
        series_id='BS-CSMCI',
        lead_months=2,
        weight=0.06,
        transform='level',
        bullish_threshold=-5,
        bearish_threshold=-15
    ),
    'eu_industrial_confidence': LeadingIndicator(
        name='EU Industrial Confidence',
        source='EUROSTAT',
        series_id='BS-ICI',
        lead_months=2,
        weight=0.06,
        transform='level',
        bullish_threshold=0,
        bearish_threshold=-10
    ),

    # === GLOBAL MARKET INDICATORS ===
    'vix': LeadingIndicator(
        name='VIX Volatility Index',
        source='YAHOO',
        series_id='^VIX',
        lead_months=1,
        weight=0.05,
        transform='level',
        bullish_threshold=15,  # Inverse! Lower is better
        bearish_threshold=25
    ),
    'sp500_momentum': LeadingIndicator(
        name='S&P 500 6-month Momentum',
        source='YAHOO',
        series_id='^GSPC',
        lead_months=3,
        weight=0.05,
        transform='yoy',
        bullish_threshold=5,
        bearish_threshold=-5
    ),

    # === COMMODITY INDICATORS ===
    'oil_price': LeadingIndicator(
        name='Brent Crude Oil',
        source='YAHOO',
        series_id='BZ=F',
        lead_months=2,
        weight=0.03,
        transform='yoy',
        bullish_threshold=-10,  # Lower oil = better for consumers
        bearish_threshold=20
    ),
    'copper_price': LeadingIndicator(
        name='Copper (Dr. Copper)',
        source='YAHOO',
        series_id='HG=F',
        lead_months=3,
        weight=0.04,
        transform='yoy',
        bullish_threshold=5,
        bearish_threshold=-10
    ),
}

# Country-specific indicator relevance
COUNTRY_INDICATOR_RELEVANCE = {
    'US': ['us_consumer_sentiment', 'us_yield_curve', 'us_initial_claims',
           'us_building_permits', 'us_pmi_manufacturing', 'us_lei', 'vix', 'sp500_momentum'],
    'DE': ['de_ifo', 'eu_economic_sentiment', 'eu_consumer_confidence',
           'eu_industrial_confidence', 'us_yield_curve', 'vix', 'oil_price'],
    'HU': ['de_ifo', 'eu_economic_sentiment', 'eu_consumer_confidence',
           'eu_industrial_confidence', 'us_yield_curve', 'vix', 'oil_price'],  # DE spillover!
    'PL': ['de_ifo', 'eu_economic_sentiment', 'eu_industrial_confidence',
           'us_yield_curve', 'vix', 'oil_price'],
    'CZ': ['de_ifo', 'eu_economic_sentiment', 'vix'],
    'SK': ['de_ifo', 'eu_economic_sentiment', 'vix'],
    'AT': ['de_ifo', 'eu_economic_sentiment', 'eu_industrial_confidence', 'vix'],
    'FR': ['eu_economic_sentiment', 'eu_consumer_confidence', 'eu_industrial_confidence',
           'de_ifo', 'us_yield_curve', 'vix'],
    'IT': ['eu_economic_sentiment', 'eu_consumer_confidence', 'eu_industrial_confidence',
           'de_ifo', 'vix', 'oil_price'],
    'ES': ['eu_economic_sentiment', 'eu_consumer_confidence', 'vix', 'oil_price'],
    'GB': ['us_consumer_sentiment', 'us_yield_curve', 'vix', 'oil_price', 'sp500_momentum'],
    'JP': ['us_pmi_manufacturing', 'us_yield_curve', 'vix', 'copper_price'],
    'CN': ['us_pmi_manufacturing', 'copper_price', 'oil_price', 'vix'],
    'EU': ['de_ifo', 'eu_economic_sentiment', 'eu_consumer_confidence',
           'eu_industrial_confidence', 'vix'],
}

# Default for countries not listed
DEFAULT_INDICATORS = ['us_yield_curve', 'vix', 'oil_price', 'copper_price']

# =============================================================================
# FRED GLOBAL ECONOMIC DATA (Non-EU countries)
# =============================================================================

FRED_GLOBAL_SERIES = {
    # === FULL DATA AVAILABILITY ===
    'US': {
        'gdp': 'A191RL1Q225SBEA',        # Real GDP Growth Rate (quarterly) - DIRECT %
        'inflation': 'CPIAUCSL',          # CPI All Urban Consumers - INDEX
        'unemployment': 'UNRATE',         # Unemployment Rate - DIRECT %
        'gdp_type': 'growth',             # Already growth rate
        'inflation_type': 'index',        # Need to calculate YoY
    },
    'GB': {
        'gdp': 'NGDPRSAXDCGBQ',           # UK Real GDP (level) - 2025 data!
        'inflation': 'GBRCPIALLMINMEI',   # UK CPI - INDEX
        'unemployment': 'LRHUTTTTGBM156S', # UK Unemployment Harmonized - 5.1% (2025!)
        'gdp_type': 'level',
        'inflation_type': 'index',
    },
    'JP': {
        'gdp': 'JPNRGDPEXP',              # Japan Real GDP Expenditure (level)
        'inflation': 'JPNCPIALLMINMEI',   # Japan CPI - INDEX (more recent)
        'unemployment': 'LRUNTTTTJPM156S', # Japan Unemployment - 2.6% (2025!)
        'gdp_type': 'level',
        'inflation_type': 'index',
    },
    'CA': {
        'gdp': 'NGDPRSAXDCCAQ',           # Canada Real GDP (level) - 2025 data!
        'inflation': 'CANCPIALLMINMEI',   # Canada CPI - INDEX (2025 data!)
        'unemployment': 'LRUNTTTTCAM156S', # Canada Unemployment
        'gdp_type': 'level',
        'inflation_type': 'index',
    },
    'AU': {
        # No GDP series available - use IMF only
        'inflation': 'AUSCPIALLQINMEI',   # Australia CPI Quarterly - INDEX (2025!)
        'unemployment': 'LRUNTTTTAUM156S', # Australia Unemployment - 4.32% (2025!)
        'inflation_type': 'index',
    },

    # === PARTIAL DATA (older or limited) ===
    'CN': {
        'gdp': 'MKTGDPCNA646NWDB',        # China GDP (level, 2024 data)
        'inflation': 'CHNCPIALLMINMEI',   # China CPI - INDEX
        'gdp_type': 'level',
        'inflation_type': 'index',
    },
    'KR': {
        'gdp': 'NAEXKP01KRQ189S',         # Korea GDP (level, 2023 data)
        'inflation': 'KORCPIALLMINMEI',   # Korea CPI - INDEX (2023)
        # No recent unemployment data
        'gdp_type': 'level',
        'inflation_type': 'index',
    },
    'MX': {
        # GDP data old
        'inflation': 'MEXCPIALLMINMEI',   # Mexico CPI - INDEX (2024)
        # Unemployment data old
        'inflation_type': 'index',
    },
    'BR': {
        # GDP data old
        'inflation': 'BRACPIALLMINMEI',   # Brazil CPI - INDEX (2025!)
        # Unemployment data old
        'inflation_type': 'index',
    },
    'IN': {
        'gdp': 'MKTGDPINA646NWDB',        # India GDP (level, 2024)
        'inflation': 'INDCPIALLMINMEI',   # India CPI - INDEX (2025!)
        'gdp_type': 'level',
        'inflation_type': 'index',
    },
}


# =============================================================================
# DATA FETCHERS
# =============================================================================

class FREDClient:
    """FRED API Client - 800,000+ idősor!"""

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or FRED_API_KEY
        self.cache: Dict[str, pd.DataFrame] = {}

    def get_series(self, series_id: str, periods: int = 60) -> Optional[pd.DataFrame]:
        """Fetch a FRED series"""
        cache_key = f"{series_id}_{periods}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        if not self.api_key:
            # Silent fail for missing API key
            return None

        start_date = (datetime.now() - timedelta(days=periods*30)).strftime('%Y-%m-%d')

        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'observation_start': start_date,
            'sort_order': 'desc',
            'limit': periods,
        }

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'observations' not in data or not data['observations']:
                return None

            df = pd.DataFrame(data['observations'])

            # Handle different response formats
            if 'date' not in df.columns:
                return None

            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.dropna(subset=['value', 'date'])

            if df.empty:
                return None

            df = df.set_index('date').sort_index()

            self.cache[cache_key] = df
            return df

        except requests.exceptions.HTTPError as e:
            # Silently handle 4xx errors (bad series ID, etc.)
            return None
        except Exception as e:
            # Only print unexpected errors
            if '429' not in str(e):  # Rate limit - don't spam
                print(f"  [FRED] Error fetching {series_id}: {e}")
            return None

    def get_latest(self, series_id: str) -> Optional[float]:
        """Get the most recent value"""
        df = self.get_series(series_id, periods=5)
        if df is not None and len(df) > 0:
            return float(df['value'].iloc[-1])
        return None

    def get_yoy_change(self, series_id: str) -> Optional[float]:
        """Get year-over-year percentage change"""
        df = self.get_series(series_id, periods=15)
        if df is not None and len(df) >= 12:
            current = df['value'].iloc[-1]
            year_ago = df['value'].iloc[-12] if len(df) >= 12 else df['value'].iloc[0]
            if year_ago != 0:
                return ((current - year_ago) / abs(year_ago)) * 100
        return None

    def get_country_economic_data(self, country: str) -> Dict[str, Any]:
        """
        Get GDP, Inflation, Unemployment for a non-EU country from FRED.

        Returns:
            {'gdp_growth': float, 'inflation': float, 'unemployment': float}
        """
        if country not in FRED_GLOBAL_SERIES:
            return {}

        config = FRED_GLOBAL_SERIES[country]
        result = {}

        # GDP Growth
        if 'gdp' in config:
            series_id = config['gdp']
            gdp_type = config.get('gdp_type', 'level')

            if gdp_type == 'growth':
                # Already a growth rate (like US A191RL1Q225SBEA)
                val = self.get_latest(series_id)
                if val is not None:
                    result['gdp_growth'] = val
            else:
                # Need to calculate YoY growth from level
                yoy = self.get_yoy_change(series_id)
                if yoy is not None:
                    result['gdp_growth'] = yoy

        # Inflation
        if 'inflation' in config:
            series_id = config['inflation']
            infl_type = config.get('inflation_type', 'index')

            if infl_type == 'rate':
                # Already an inflation rate
                val = self.get_latest(series_id)
                if val is not None:
                    result['inflation'] = val
            elif infl_type == 'mom':
                # Month-over-month, need to annualize (rough: *12)
                val = self.get_latest(series_id)
                if val is not None:
                    result['inflation'] = val * 12
            else:
                # Index, calculate YoY
                yoy = self.get_yoy_change(series_id)
                if yoy is not None:
                    result['inflation'] = yoy

        # Unemployment
        if 'unemployment' in config:
            val = self.get_latest(config['unemployment'])
            if val is not None:
                result['unemployment'] = val

        return result


class EurostatConfidenceClient:
    """Eurostat EU Confidence Indicators"""

    BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ei_bssi_m_r2"

    def __init__(self):
        self.cache: Dict[str, float] = {}
        self.session = requests.Session()

    def get_indicator(self, indicator: str, country: str = 'EU27_2020') -> Optional[float]:
        """
        Get EU confidence indicators

        Indicators:
        - BS-CSMCI: Consumer Confidence
        - BS-ICI: Industrial Confidence
        - BS-SCI: Services Confidence
        - BS-ESI-I: Economic Sentiment Indicator
        """
        cache_key = f"{indicator}_{country}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        params = {
            'format': 'JSON',
            'geo': country,
            'indic': indicator,
            's_adj': 'SA',
            'lastTimePeriod': 1
        }

        try:
            res = self.session.get(self.BASE_URL, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()

            if 'value' in data and data['value']:
                value = float(list(data['value'].values())[0])
                self.cache[cache_key] = value
                return value

        except Exception as e:
            print(f"  [Eurostat] Error fetching {indicator}: {e}")
        return None

    def get_economic_sentiment(self, country: str = 'EU27_2020') -> Optional[float]:
        return self.get_indicator('BS-ESI-I', country)

    def get_consumer_confidence(self, country: str = 'EU27_2020') -> Optional[float]:
        return self.get_indicator('BS-CSMCI', country)

    def get_industrial_confidence(self, country: str = 'EU27_2020') -> Optional[float]:
        return self.get_indicator('BS-ICI', country)


class YahooFinanceClient:
    """Yahoo Finance for market indicators"""

    def __init__(self):
        self.cache: Dict[str, Any] = {}

    def get_quote(self, symbol: str) -> Optional[float]:
        """Get current price/level"""
        if symbol in self.cache:
            cached = self.cache[symbol]
            if (datetime.now() - cached['time']).seconds < 3600:
                return cached['value']

        try:
            import yfinance as yf
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='5d')
            if len(data) > 0:
                value = float(data['Close'].iloc[-1])
                self.cache[symbol] = {'value': value, 'time': datetime.now()}
                return value
        except Exception as e:
            print(f"  [Yahoo] Error fetching {symbol}: {e}")
        return None

    def get_yoy_change(self, symbol: str) -> Optional[float]:
        """Get year-over-year change"""
        try:
            import yfinance as yf
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='1y')
            if len(data) >= 200:
                current = data['Close'].iloc[-1]
                year_ago = data['Close'].iloc[0]
                if year_ago != 0:
                    return ((current - year_ago) / year_ago) * 100
        except:
            pass
        return None

    def get_vix(self) -> Optional[float]:
        return self.get_quote('^VIX')


class GoogleTrendsClient:
    """Google Trends for real-time sentiment (OPTIONAL)"""

    def __init__(self):
        self.pytrends = None
        self._available = False
        self._init_pytrends()

    def _init_pytrends(self):
        try:
            from pytrends.request import TrendReq
            self.pytrends = TrendReq(hl='en-US', tz=360)
            self._available = True
        except ImportError:
            print("  [GTrends] pytrends not installed (optional)")

    @property
    def available(self) -> bool:
        return self._available

    def get_recession_fear_index(self, geo: str = 'US') -> Optional[float]:
        """Composite recession fear from keywords"""
        if not self._available:
            return None

        try:
            keywords = ['recession', 'layoffs', 'unemployment']
            self.pytrends.build_payload(keywords, geo=geo, timeframe='today 3-m')
            df = self.pytrends.interest_over_time()

            if len(df) > 0:
                # Average of last week
                recent = df.tail(7)[keywords].mean().mean()
                return float(recent)
        except Exception as e:
            print(f"  [GTrends] Error: {e}")
        return None


# =============================================================================
# ULTIMATE FORECASTER ENGINE
# =============================================================================

class UltimateForecaster:
    """
    ULTIMATE FORECASTER - Kombinálja az összes adatforrást!

    Features:
    - Leading indicators ensemble scoring
    - Phillips Curve + Okun's Law (SAJÁT FORECASTER-ből)
    - IMF WEO forecasts integration
    - OECD CLI integration
    - Recession probability estimation
    - Country-specific analysis
    - Quarterly/Monthly forecasting (MONOPOL!)
    """

    # ISO2 -> ISO3 mapping for DBnomics
    COUNTRY_ISO3 = {
        'HU': 'HUN', 'DE': 'DEU', 'US': 'USA', 'GB': 'GBR', 'FR': 'FRA',
        'IT': 'ITA', 'ES': 'ESP', 'PL': 'POL', 'CZ': 'CZE', 'SK': 'SVK',
        'AT': 'AUT', 'NL': 'NLD', 'BE': 'BEL', 'CH': 'CHE', 'SE': 'SWE',
        'DK': 'DNK', 'NO': 'NOR', 'FI': 'FIN', 'PT': 'PRT', 'GR': 'GRC',
        'IE': 'IRL', 'RO': 'ROU', 'BG': 'BGR', 'HR': 'HRV', 'SI': 'SVN',
        'EE': 'EST', 'LV': 'LVA', 'LT': 'LTU', 'JP': 'JPN', 'CN': 'CHN',
        'KR': 'KOR', 'AU': 'AUS', 'CA': 'CAN', 'MX': 'MEX', 'BR': 'BRA',
        'IN': 'IND', 'TR': 'TUR', 'ZA': 'ZAF', 'AR': 'ARG',
    }

    def __init__(self):
        print("🚀 Initializing ULTIMATE FORECASTER...")

        # Initialize data clients
        self.fred = FREDClient()
        self.eurostat = EurostatConfidenceClient()
        self.yahoo = YahooFinanceClient()
        self.gtrends = GoogleTrendsClient()

        # Data storage
        self.indicators: Dict[str, float] = {}
        self.indicator_signals: Dict[str, str] = {}

        # Import SAJÁT FORECASTER if available
        self.sajat_available = False
        try:
            from .sajat_forecaster_cache import get_sajat_forecast, get_combined_forecast
            self.get_sajat_forecast = get_sajat_forecast
            self.get_combined_forecast = get_combined_forecast
            self.sajat_available = True
            print("   ✓ SAJÁT FORECASTER integrated")
        except ImportError:
            print("   ⚠ SAJÁT FORECASTER not available")

        # Import DBnomics for IMF WEO
        self.dbnomics_available = False
        try:
            from .dbnomics_client import DBnomicsClient
            self.dbnomics = DBnomicsClient()
            self.dbnomics_available = True
            print("   ✓ DBnomics (IMF WEO) integrated")
        except ImportError:
            print("   ⚠ DBnomics not available")

        # Import OECD CLI
        self.oecd_available = False
        try:
            from .oecd_client import OECDClient
            self.oecd = OECDClient()
            self.oecd_available = True
            print("   ✓ OECD CLI integrated")
        except ImportError:
            print("   ⚠ OECD CLI not available")

        print(f"   ✓ FRED Client ({'API key set' if self.fred.api_key else 'no API key'})")
        print("   ✓ Eurostat Confidence Client")
        print("   ✓ Yahoo Finance Client")
        print(f"   {'✓' if self.gtrends.available else '⚠'} Google Trends Client")
        print("🏆 ULTIMATE FORECASTER ready!")

    def fetch_all_indicators(self) -> Dict[str, Any]:
        """Fetch all leading indicators"""

        print("\n📊 Fetching Leading Indicators...")
        results = {}

        # === FRED Indicators ===
        print("\n[FRED - US Economic Data]")

        fred_indicators = [
            ('us_consumer_sentiment', 'UMCSENT', 'level'),
            ('us_yield_curve', 'T10Y2Y', 'level'),
            ('us_initial_claims', 'IC4WSA', 'level'),
            ('us_building_permits', 'PERMIT', 'level'),
            ('us_lei', 'USPHCI', 'level'),
            ('de_ifo', 'BSCICP02DEM460S', 'level'),
            # de_zew removed - series doesn't exist on FRED, using ifo + Eurostat instead
        ]

        for key, series_id, transform in fred_indicators:
            if transform == 'level':
                val = self.fred.get_latest(series_id)
            else:
                val = self.fred.get_yoy_change(series_id)

            if val is not None:
                results[key] = val
                config = LEADING_INDICATORS.get(key)
                if config:
                    signal = self._get_signal(val, config)
                    print(f"  {signal} {config.name}: {val:.1f}")

        # === Eurostat ===
        print("\n[Eurostat - EU Confidence]")

        val = self.eurostat.get_economic_sentiment()
        if val:
            results['eu_economic_sentiment'] = val
            print(f"  {'🟢' if val > 100 else '🟡' if val > 95 else '🔴'} Economic Sentiment: {val:.1f}")

        val = self.eurostat.get_consumer_confidence()
        if val:
            results['eu_consumer_confidence'] = val
            print(f"  {'🟢' if val > -5 else '🟡' if val > -15 else '🔴'} Consumer Confidence: {val:.1f}")

        val = self.eurostat.get_industrial_confidence()
        if val:
            results['eu_industrial_confidence'] = val
            print(f"  {'🟢' if val > 0 else '🟡' if val > -10 else '🔴'} Industrial Confidence: {val:.1f}")

        # === Yahoo Finance ===
        print("\n[Yahoo Finance - Market Signals]")

        val = self.yahoo.get_vix()
        if val:
            results['vix'] = val
            print(f"  {'🟢' if val < 15 else '🟡' if val < 25 else '🔴'} VIX: {val:.1f}")

        val = self.yahoo.get_yoy_change('^GSPC')
        if val:
            results['sp500_momentum'] = val
            print(f"  {'🟢' if val > 5 else '🟡' if val > -5 else '🔴'} S&P 500 YoY: {val:+.1f}%")

        val = self.yahoo.get_yoy_change('BZ=F')
        if val:
            results['oil_price'] = val
            print(f"  {'🟢' if val < -10 else '🟡' if val < 20 else '🔴'} Oil YoY: {val:+.1f}%")

        val = self.yahoo.get_yoy_change('HG=F')
        if val:
            results['copper_price'] = val
            print(f"  {'🟢' if val > 5 else '🟡' if val > -10 else '🔴'} Copper YoY: {val:+.1f}%")

        # === Google Trends (optional) ===
        if self.gtrends.available:
            print("\n[Google Trends - Sentiment]")
            val = self.gtrends.get_recession_fear_index('US')
            if val:
                results['recession_fear_us'] = val
                print(f"  {'🔴' if val > 50 else '🟡' if val > 25 else '🟢'} Recession Fear Index: {val:.0f}")

        self.indicators = results
        return results

    def _get_signal(self, value: float, config: LeadingIndicator) -> str:
        """Get signal emoji based on thresholds"""
        # Handle inverse indicators (lower is better)
        if 'claims' in config.name.lower() or 'vix' in config.name.lower():
            if value < config.bullish_threshold:
                return '🟢'
            elif value > config.bearish_threshold:
                return '🔴'
            return '🟡'
        else:
            if value > config.bullish_threshold:
                return '🟢'
            elif value < config.bearish_threshold:
                return '🔴'
            return '🟡'

    def calculate_composite_score(self, country: str = 'US') -> Dict[str, Any]:
        """
        Calculate composite leading indicator score for a country

        Returns:
            - composite_score: -100 to +100
            - recession_probability: 0-100%
            - gdp_growth_signal: expected direction
            - confidence: data quality score
        """

        if not self.indicators:
            self.fetch_all_indicators()

        # Get relevant indicators for country
        relevant = COUNTRY_INDICATOR_RELEVANCE.get(country.upper(), DEFAULT_INDICATORS)

        scores = []
        weights = []
        details = []

        for ind_id in relevant:
            if ind_id not in self.indicators:
                continue

            config = LEADING_INDICATORS.get(ind_id)
            if not config:
                continue

            value = self.indicators[ind_id]

            # Normalize to -100 to +100 scale
            bull = config.bullish_threshold
            bear = config.bearish_threshold

            # Handle inverse indicators
            is_inverse = 'claims' in config.name.lower() or 'vix' in config.name.lower() or 'oil' in ind_id

            if is_inverse:
                # Lower is better
                if value <= bull:
                    normalized = 50 + (bull - value) / bull * 50
                elif value >= bear:
                    normalized = -50 - (value - bear) / bear * 50
                else:
                    # Linear interpolation
                    normalized = 50 - (value - bull) / (bear - bull) * 100
            else:
                # Higher is better
                if value >= bull:
                    normalized = 50 + min(50, (value - bull) / (bull * 0.2) * 50)
                elif value <= bear:
                    normalized = -50 - min(50, (bear - value) / (bear * 0.2) * 50)
                else:
                    # Linear interpolation
                    normalized = (value - bear) / (bull - bear) * 100 - 50

            normalized = max(-100, min(100, normalized))

            scores.append(normalized)
            weights.append(config.weight)
            details.append({
                'indicator': config.name,
                'value': value,
                'normalized': normalized,
                'weight': config.weight
            })

        if not scores:
            return {
                'composite_score': 0,
                'recession_probability': 50,
                'gdp_growth_signal': 'UNKNOWN',
                'confidence': 0,
                'country': country,
                'indicators_used': 0
            }

        # Weighted average
        weights = np.array(weights)
        weights = weights / weights.sum()
        composite = np.average(scores, weights=weights)

        # Recession probability model
        if composite < -30:
            recession_prob = 70 + min(25, (-composite - 30) * 0.8)
        elif composite < -15:
            recession_prob = 50 + (-composite - 15) * 1.3
        elif composite < 0:
            recession_prob = 30 + (-composite) * 1.3
        elif composite < 15:
            recession_prob = 15 + (15 - composite)
        else:
            recession_prob = max(5, 15 - (composite - 15) * 0.5)

        recession_prob = max(5, min(95, recession_prob))

        # GDP signal
        if composite > 25:
            gdp_signal = 'STRONG_GROWTH'
        elif composite > 10:
            gdp_signal = 'MODERATE_GROWTH'
        elif composite > -5:
            gdp_signal = 'STAGNATION'
        elif composite > -20:
            gdp_signal = 'SLOWDOWN'
        else:
            gdp_signal = 'CONTRACTION'

        # Confidence based on data availability
        confidence = min(95, len(scores) * 12)

        return {
            'composite_score': round(composite, 1),
            'recession_probability': round(recession_prob, 0),
            'gdp_growth_signal': gdp_signal,
            'confidence': confidence,
            'country': country,
            'indicators_used': len(scores),
            'indicator_details': details,
            'timestamp': datetime.now().isoformat()
        }

    def get_ultimate_forecast(
        self,
        country: str,
        indicator: str,  # 'gdp' or 'inflation'
        year: int,
        quarter: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get ULTIMATE FORECAST combining all sources!

        Combines:
        1. SAJÁT FORECASTER (Phillips Curve, Okun's Law)
        2. IMF WEO (via DBnomics)
        3. Leading indicators adjustment
        4. OECD CLI signal

        Args:
            country: ISO 2 country code
            indicator: 'gdp' or 'inflation'
            year: Target year
            quarter: Optional quarter (1-4) for quarterly forecast

        Returns:
            Ultimate forecast with full transparency
        """

        country = country.upper()
        result = {
            'country': country,
            'indicator': indicator,
            'year': year,
            'quarter': quarter,
            'sources': {},
            'weights': {},
            'ultimate_forecast': None,
            'confidence': 0,
            'is_quarterly': quarter is not None
        }

        forecasts = []
        weights = []

        # 1. SAJÁT FORECASTER (if available)
        if self.sajat_available:
            try:
                sajat = self.get_sajat_forecast(country, indicator, year, quarter=quarter)
                if sajat and sajat.get('value') is not None:
                    val = sajat['value']
                    result['sources']['sajat'] = val
                    forecasts.append(val)
                    # SAJÁT gets higher weight for GDP (backtesting winner)
                    w = 0.35 if indicator == 'gdp' else 0.25
                    weights.append(w)
                    result['weights']['sajat'] = w

                    # MONOPOL bonus for quarterly!
                    if quarter:
                        result['quarterly_monopol'] = True
            except Exception as e:
                print(f"  [SAJÁT] Error: {e}")

        # 2. IMF WEO (via DBnomics)
        if self.dbnomics_available and quarter is None:  # IMF only has annual
            try:
                # Convert ISO2 to ISO3 for DBnomics
                iso3 = self.COUNTRY_ISO3.get(country, country)
                imf = self.dbnomics.get_imf_forecast(iso3, indicator, year)
                if imf and imf.get('forecast') is not None:
                    val = imf['forecast']
                    result['sources']['imf_weo'] = val
                    forecasts.append(val)
                    # IMF gets higher weight for inflation (backtesting winner)
                    w = 0.30 if indicator == 'inflation' else 0.25
                    weights.append(w)
                    result['weights']['imf_weo'] = w
            except Exception as e:
                print(f"  [IMF] Error: {e}")

        # 3. Leading indicators adjustment
        composite = self.calculate_composite_score(country)
        if composite['confidence'] >= 40:
            # Convert composite score to GDP adjustment
            # Score of +50 → +0.5% GDP, Score of -50 → -0.5% GDP
            adjustment = composite['composite_score'] / 100
            result['sources']['leading_indicators'] = {
                'adjustment': round(adjustment, 2),
                'composite_score': composite['composite_score'],
                'signal': composite['gdp_growth_signal'],
                'recession_prob': composite['recession_probability']
            }

            # Apply adjustment to existing forecasts
            if forecasts and indicator == 'gdp':
                base = np.mean(forecasts)
                adjusted = base + adjustment
                result['leading_indicator_adjustment'] = round(adjustment, 2)

        # 4. OECD CLI (if available)
        if self.oecd_available:
            try:
                cli = self.oecd.get_cli(country)
                if cli:
                    result['sources']['oecd_cli'] = cli
                    # CLI doesn't directly translate to forecast, but affects confidence
                    if cli > 100:
                        result['oecd_signal'] = 'EXPANSION'
                    elif cli < 99:
                        result['oecd_signal'] = 'CONTRACTION'
                    else:
                        result['oecd_signal'] = 'STABLE'
            except:
                pass

        # 5. FRED Global Data (for non-EU countries: US, GB, JP, CA, AU, CN, KR, MX, BR, IN)
        if country in FRED_GLOBAL_SERIES:
            try:
                fred_data = self.fred.get_country_economic_data(country)
                if fred_data:
                    result['sources']['fred_current'] = fred_data

                    # Use FRED data to create a forecast estimate
                    if indicator == 'gdp' and 'gdp_growth' in fred_data:
                        current_gdp = fred_data['gdp_growth']
                        # Project forward with leading indicator adjustment
                        adj = result.get('leading_indicator_adjustment', 0)
                        fred_forecast = current_gdp + adj * 0.3  # Slight adjustment
                        result['sources']['fred_gdp_current'] = current_gdp
                        forecasts.append(fred_forecast)
                        w = 0.25  # FRED gets good weight for non-EU
                        weights.append(w)
                        result['weights']['fred'] = w

                    elif indicator == 'inflation' and 'inflation' in fred_data:
                        current_infl = fred_data['inflation']
                        # Project inflation with slight decay toward target
                        target = 2.0  # Most central banks target 2%
                        fred_forecast = current_infl * 0.85 + target * 0.15
                        result['sources']['fred_inflation_current'] = current_infl
                        forecasts.append(fred_forecast)
                        w = 0.30  # FRED inflation is reliable
                        weights.append(w)
                        result['weights']['fred'] = w

                    elif indicator == 'unemployment' and 'unemployment' in fred_data:
                        current_unemp = fred_data['unemployment']
                        # Unemployment tends to be sticky, project with small adjustment
                        adj = result.get('leading_indicator_adjustment', 0)
                        # Negative leading indicator = higher unemployment
                        fred_forecast = current_unemp - adj * 0.1
                        result['sources']['fred_unemployment_current'] = current_unemp
                        forecasts.append(fred_forecast)
                        w = 0.35  # FRED unemployment is very reliable
                        weights.append(w)
                        result['weights']['fred'] = w

            except Exception as e:
                print(f"  [FRED Global] Error for {country}: {e}")

        # Calculate ULTIMATE forecast
        if forecasts:
            weights = np.array(weights)
            weights = weights / weights.sum()
            ultimate = np.average(forecasts, weights=weights)

            # Apply leading indicator adjustment for GDP
            if indicator == 'gdp' and 'leading_indicator_adjustment' in result:
                ultimate += result['leading_indicator_adjustment'] * 0.5  # 50% of adjustment

            result['ultimate_forecast'] = round(ultimate, 2)
            result['confidence'] = min(90, 40 + len(forecasts) * 15 + composite['confidence'] * 0.3)

            # Add scenario ranges
            result['scenarios'] = {
                'pessimistic': round(ultimate - 0.8, 2),
                'realistic': round(ultimate, 2),
                'optimistic': round(ultimate + 0.8, 2)
            }

        return result

    def run_full_analysis(self, countries: List[str] = None) -> Dict[str, Any]:
        """Run complete analysis for multiple countries"""

        if countries is None:
            countries = ['US', 'DE', 'HU', 'GB', 'FR', 'PL']

        print("=" * 70)
        print("🏆 ULTIMATE FORECASTER - Full Analysis")
        print("=" * 70)

        # Fetch all indicators
        self.fetch_all_indicators()

        # Analyze each country
        results = {}

        print("\n" + "=" * 70)
        print("📈 COUNTRY ANALYSIS")
        print("=" * 70)

        for country in countries:
            signal = self.calculate_composite_score(country)
            results[country] = {
                'composite': signal,
                'gdp_forecast': self.get_ultimate_forecast(country, 'gdp', 2026),
                'inflation_forecast': self.get_ultimate_forecast(country, 'inflation', 2026),
            }

            print(f"\n🌍 {country}:")
            print(f"   Composite Score: {signal['composite_score']:+.1f}")
            print(f"   GDP Signal: {signal['gdp_growth_signal']}")
            print(f"   Recession Prob: {signal['recession_probability']:.0f}%")
            print(f"   Data Confidence: {signal['confidence']}%")

            gdp = results[country]['gdp_forecast']
            if gdp.get('ultimate_forecast'):
                print(f"   🔮 GDP 2026 Forecast: {gdp['ultimate_forecast']:.2f}%")
                if gdp.get('quarterly_monopol'):
                    print(f"      🏆 QUARTERLY FORECASTING AVAILABLE!")

        # Global summary
        print("\n" + "=" * 70)
        print("📊 GLOBAL SUMMARY")
        print("=" * 70)

        avg_score = np.mean([r['composite']['composite_score'] for r in results.values()])
        avg_recession = np.mean([r['composite']['recession_probability'] for r in results.values()])

        print(f"\nGlobal Average Composite: {avg_score:+.1f}")
        print(f"Global Recession Risk: {avg_recession:.0f}%")

        if avg_score > 15:
            print("\n🟢 OVERALL: Economic expansion expected")
        elif avg_score > -5:
            print("\n🟡 OVERALL: Mixed signals, watch closely")
        else:
            print("\n🔴 OVERALL: Warning signs, potential slowdown")

        return results


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_ultimate_forecaster: Optional[UltimateForecaster] = None

def get_ultimate_forecaster() -> UltimateForecaster:
    """Get singleton instance of UltimateForecaster"""
    global _ultimate_forecaster
    if _ultimate_forecaster is None:
        _ultimate_forecaster = UltimateForecaster()
    return _ultimate_forecaster

def get_ultimate_gdp_forecast(country: str, year: int, quarter: int = None) -> Dict[str, Any]:
    """Quick GDP forecast"""
    return get_ultimate_forecaster().get_ultimate_forecast(country, 'gdp', year, quarter)

def get_ultimate_inflation_forecast(country: str, year: int, quarter: int = None) -> Dict[str, Any]:
    """Quick inflation forecast"""
    return get_ultimate_forecaster().get_ultimate_forecast(country, 'inflation', year, quarter)

def get_ultimate_unemployment_forecast(country: str, year: int, quarter: int = None) -> Dict[str, Any]:
    """Quick unemployment forecast"""
    return get_ultimate_forecaster().get_ultimate_forecast(country, 'unemployment', year, quarter)

def get_recession_probability(country: str) -> float:
    """Get recession probability for a country"""
    signal = get_ultimate_forecaster().calculate_composite_score(country)
    return signal['recession_probability']


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    forecaster = UltimateForecaster()
    results = forecaster.run_full_analysis(['US', 'DE', 'HU', 'GB', 'FR', 'PL'])

    print("\n" + "=" * 70)
    print("🔮 DETAILED FORECASTS")
    print("=" * 70)

    for country in ['HU', 'DE', 'US']:
        print(f"\n📊 {country} GDP 2026:")
        gdp = results[country]['gdp_forecast']
        print(f"   Ultimate Forecast: {gdp.get('ultimate_forecast', 'N/A')}%")
        print(f"   Sources: {list(gdp.get('sources', {}).keys())}")
        if 'scenarios' in gdp:
            print(f"   Pessimistic: {gdp['scenarios']['pessimistic']}%")
            print(f"   Realistic: {gdp['scenarios']['realistic']}%")
            print(f"   Optimistic: {gdp['scenarios']['optimistic']}%")
