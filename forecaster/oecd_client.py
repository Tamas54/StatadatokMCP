"""
OECD CLI (Composite Leading Indicator) Client

The CLI is a leading economic indicator that predicts turning points
in business cycles. Values above 100 indicate expansion, below 100
indicate contraction.

Based on methodology from Nexonomics master_economic_dashboard_v5.py
"""

import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from typing import Optional, Dict, List


class OECDClient:
    """
    OECD SDMX REST API client for economic indicators.

    Primary indicator: Composite Leading Indicator (CLI)
    - CLI > 100: Economic expansion
    - CLI < 100: Economic contraction
    - CLI trending up: Improving outlook
    - CLI trending down: Worsening outlook
    """

    BASE_URL = "https://sdmx.oecd.org/public/rest/data"

    # Country code mapping (2-letter to 3-letter ISO)
    COUNTRY_MAP = {
        'DE': 'DEU', 'FR': 'FRA', 'IT': 'ITA', 'ES': 'ESP',
        'NL': 'NLD', 'BE': 'BEL', 'AT': 'AUT', 'PL': 'POL',
        'CZ': 'CZE', 'HU': 'HUN', 'SE': 'SWE', 'DK': 'DNK',
        'FI': 'FIN', 'PT': 'PRT', 'GR': 'GRC', 'IE': 'IRL',
        'US': 'USA', 'GB': 'GBR', 'UK': 'GBR', 'JP': 'JPN',
        'CN': 'CHN', 'CA': 'CAN', 'AU': 'AUS', 'KR': 'KOR',
        'MX': 'MEX', 'BR': 'BRA', 'IN': 'IND', 'RU': 'RUS',
        'ZA': 'ZAF', 'TR': 'TUR', 'CH': 'CHE', 'NO': 'NOR',
    }

    # Countries with CLI data available
    CLI_COUNTRIES = {
        'DEU', 'FRA', 'ITA', 'ESP', 'NLD', 'BEL', 'AUT', 'POL',
        'CZE', 'HUN', 'SWE', 'DNK', 'FIN', 'PRT', 'GRC', 'IRL',
        'USA', 'GBR', 'JPN', 'CHN', 'CAN', 'AUS', 'KOR', 'MEX',
        'BRA', 'IND', 'RUS', 'ZAF', 'TUR', 'CHE', 'NOR',
    }

    def __init__(self, cache_ttl: int = 300):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'OpScanner/5.2'
        })

        self.cache: Dict[str, Dict] = {}
        self.cache_ttl = cache_ttl

        # Rate limit tracking (60 requests/hour)
        self.request_times: List[datetime] = []
        self.rate_limit = 60

    def _normalize_country(self, country: str) -> str:
        """Convert 2-letter to 3-letter country code"""
        country_upper = country.upper()
        return self.COUNTRY_MAP.get(country_upper, country_upper)

    def _check_cache(self, key: str) -> Optional[float]:
        """Check cache with TTL"""
        if key in self.cache:
            cached = self.cache[key]
            age = (datetime.now() - cached['timestamp']).total_seconds()
            if age < self.cache_ttl:
                return cached['value']
        return None

    def _set_cache(self, key: str, value: float) -> None:
        """Cache a value"""
        self.cache[key] = {
            'value': value,
            'timestamp': datetime.now()
        }

    def _check_rate_limit(self) -> bool:
        """Check if we're within OECD rate limits (60/hour)"""
        now = datetime.now()
        # Remove requests older than 1 hour
        self.request_times = [
            t for t in self.request_times
            if (now - t).total_seconds() < 3600
        ]

        if len(self.request_times) >= self.rate_limit:
            print(f"  [OECD] Rate limit reached ({self.rate_limit}/hour)")
            return False
        return True

    def get_cli(self, country: str, periods: int = 6) -> Optional[Dict]:
        """
        Get Composite Leading Indicator for a country.

        Args:
            country: 2 or 3 letter country code
            periods: Number of monthly observations to fetch

        Returns:
            Dict with 'value', 'period', 'history', 'trend', 'source'
        """
        country_code = self._normalize_country(country)

        if country_code not in self.CLI_COUNTRIES:
            return None

        cache_key = f"oecd_cli_{country_code}"
        cached = self._check_cache(cache_key)
        if cached is not None:
            return cached

        if not self._check_rate_limit():
            # Return stale cache if available
            if cache_key in self.cache:
                return self.cache[cache_key]['value']
            return None

        # OECD CLI URL - SDMX REST API 2024 format
        # Dataflow: OECD.SDD.STES,DSD_STES@DF_CLI
        # Dimensions: {country}.M.LI...AA.IX..H
        # M = Monthly, LI = Leading Indicator, AA = Amplitude Adjusted, IX = Index, H = current methodology
        dataflow = "OECD.SDD.STES,DSD_STES@DF_CLI"
        dimension_path = f"{country_code}.M.LI...AA.IX..H"

        url = f"{self.BASE_URL}/{dataflow}/{dimension_path}"

        params = {
            "lastNObservations": periods,
            "dimensionAtObservation": "AllDimensions",
            "format": "csvfilewithlabels"
        }

        try:
            self.request_times.append(datetime.now())

            res = self.session.get(url, params=params, timeout=15)
            res.raise_for_status()

            df = pd.read_csv(StringIO(res.text))

            if 'OBS_VALUE' not in df.columns or len(df) == 0:
                print(f"  [OECD] No CLI data for {country_code}")
                return None

            # Get latest value and history
            values = df['OBS_VALUE'].tolist()
            latest = float(values[-1]) if values else None

            if latest is None:
                return None

            # Get period info
            period = None
            if 'TIME_PERIOD' in df.columns:
                period = df.iloc[-1]['TIME_PERIOD']

            # Calculate trend (3-month change)
            trend = None
            trend_direction = None
            if len(values) >= 3:
                trend = values[-1] - values[-3]
                if trend > 0.2:
                    trend_direction = "improving"
                elif trend < -0.2:
                    trend_direction = "worsening"
                else:
                    trend_direction = "stable"

            result = {
                'value': round(latest, 2),
                'period': period,
                'history': [round(v, 2) for v in values],
                'trend': round(trend, 2) if trend else None,
                'trend_direction': trend_direction,
                'momentum': 'expansion' if latest > 100 else 'contraction',
                'source': 'oecd-cli',
                'country': country_code
            }

            self._set_cache(cache_key, result)
            return result

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status == 404:
                print(f"  [OECD] No CLI data for {country_code}")
            elif status == 422:
                print(f"  [OECD] Invalid query for {country_code}")
            else:
                print(f"  [OECD] HTTP {status} for {country_code}")
            return None
        except Exception as e:
            print(f"  [OECD] Error: {e}")
            return None

    def get_global_momentum(self, country: str) -> str:
        """
        Get simple momentum indicator for a country.

        Returns:
            "EXPANSION" if CLI > 100, "CONTRACTION" otherwise
        """
        cli_data = self.get_cli(country)
        if cli_data and cli_data.get('value'):
            return "EXPANSION" if cli_data['value'] > 100 else "CONTRACTION"
        return "UNKNOWN"

    def is_trending_up(self, country: str) -> Optional[bool]:
        """Check if CLI trend is positive (improving outlook)"""
        cli_data = self.get_cli(country)
        if cli_data and cli_data.get('trend') is not None:
            return cli_data['trend'] > 0
        return None


# Singleton instance
_client_instance = None


def get_oecd_client() -> OECDClient:
    """Get singleton OECDClient instance"""
    global _client_instance
    if _client_instance is None:
        _client_instance = OECDClient()
    return _client_instance


# Test
if __name__ == "__main__":
    print("=" * 50)
    print("OECD CLI Client Test")
    print("=" * 50)

    client = get_oecd_client()

    test_countries = ['DE', 'HU', 'FR', 'US', 'JP']

    for country in test_countries:
        print(f"\n{country}:")
        data = client.get_cli(country)
        if data:
            print(f"  CLI: {data['value']} ({data['period']})")
            print(f"  Momentum: {data['momentum'].upper()}")
            print(f"  Trend: {data['trend_direction']} ({data['trend']:+.2f})")
            print(f"  History: {data['history'][-3:]}")
        else:
            print("  No data")
