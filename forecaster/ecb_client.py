"""
ECB (European Central Bank) Data Client

Provides eurozone-level economic data for confirmation/triangulation.

Based on methodology from Nexonomics master_economic_dashboard_v5.py
"""

import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from typing import Optional, Dict


class ECBClient:
    """
    ECB SDMX REST API client for eurozone economic indicators.

    Primary use: Get eurozone-level inflation (HICP) for comparison
    with national-level data from Eurostat.
    """

    # ECB Data API endpoint
    BASE_URL = "https://data-api.ecb.europa.eu/service/data"

    def __init__(self, cache_ttl: int = 300):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'OpScanner/5.2'
        })

        self.cache: Dict[str, Dict] = {}
        self.cache_ttl = cache_ttl

    def _check_cache(self, key: str) -> Optional[float]:
        """Check cache with TTL"""
        if key in self.cache:
            cached = self.cache[key]
            age = (datetime.now() - cached['timestamp']).total_seconds()
            if age < self.cache_ttl:
                return cached['value']
        return None

    def _set_cache(self, key: str, value) -> None:
        """Cache a value"""
        self.cache[key] = {
            'value': value,
            'timestamp': datetime.now()
        }

    def get_eurozone_hicp(self, periods: int = 6) -> Optional[Dict]:
        """
        Get Eurozone HICP (Harmonized Index of Consumer Prices) inflation.

        This is the ECB's official inflation measure for the eurozone.

        Returns:
            Dict with 'value', 'period', 'history', 'source'
        """
        cache_key = "ecb_hicp_eurozone"
        cached = self._check_cache(cache_key)
        if cached is not None:
            return cached

        # ECB SDMX query: HICP, Monthly, Euro area (U2), All-items, Annual rate of change
        # Format: ICP/M.U2.N.000000.4.ANR
        url = f"{self.BASE_URL}/ICP/M.U2.N.000000.4.ANR"

        params = {
            'lastNObservations': periods,
            'format': 'csvdata'
        }

        try:
            res = self.session.get(url, params=params, timeout=10)
            res.raise_for_status()

            df = pd.read_csv(StringIO(res.text))

            if 'OBS_VALUE' not in df.columns or len(df) == 0:
                print("  [ECB] No HICP data available")
                return None

            values = df['OBS_VALUE'].tolist()
            latest = float(values[-1]) if values else None

            if latest is None:
                return None

            # Get period
            period = None
            if 'TIME_PERIOD' in df.columns:
                period = df.iloc[-1]['TIME_PERIOD']

            result = {
                'value': round(latest, 2),
                'period': period,
                'history': [round(v, 2) for v in values],
                'source': 'ecb-hicp'
            }

            self._set_cache(cache_key, result)
            return result

        except requests.exceptions.HTTPError as e:
            print(f"  [ECB] HTTP {e.response.status_code}")
            return None
        except Exception as e:
            print(f"  [ECB] Error: {e}")
            return None

    def get_eurozone_core_hicp(self) -> Optional[Dict]:
        """
        Get Eurozone Core HICP (excluding energy and food).

        This is often more stable and useful for trend analysis.
        """
        cache_key = "ecb_core_hicp_eurozone"
        cached = self._check_cache(cache_key)
        if cached is not None:
            return cached

        # Core HICP: All-items excluding energy and unprocessed food
        # COICOP: TOT_X_NRG_FOOD
        url = f"{self.BASE_URL}/ICP/M.U2.N.TOT_X_NRG_FOOD.4.ANR"

        params = {
            'lastNObservations': 6,
            'format': 'csvdata'
        }

        try:
            res = self.session.get(url, params=params, timeout=10)
            res.raise_for_status()

            df = pd.read_csv(StringIO(res.text))

            if 'OBS_VALUE' not in df.columns or len(df) == 0:
                return None

            values = df['OBS_VALUE'].tolist()
            latest = float(values[-1])

            period = df.iloc[-1]['TIME_PERIOD'] if 'TIME_PERIOD' in df.columns else None

            result = {
                'value': round(latest, 2),
                'period': period,
                'history': [round(v, 2) for v in values],
                'source': 'ecb-core-hicp'
            }

            self._set_cache(cache_key, result)
            return result

        except Exception as e:
            print(f"  [ECB] Core HICP error: {e}")
            return None

    def get_policy_rate(self) -> Optional[Dict]:
        """
        Get ECB main refinancing rate (policy rate).

        Useful for interest rate markets.
        """
        cache_key = "ecb_policy_rate"
        cached = self._check_cache(cache_key)
        if cached is not None:
            return cached

        # ECB key interest rates - Main refinancing operations
        url = f"{self.BASE_URL}/FM/D.U2.EUR.4F.KR.MRR_FR.LEV"

        params = {
            'lastNObservations': 12,
            'format': 'csvdata'
        }

        try:
            res = self.session.get(url, params=params, timeout=10)
            res.raise_for_status()

            df = pd.read_csv(StringIO(res.text))

            if 'OBS_VALUE' not in df.columns or len(df) == 0:
                return None

            values = df['OBS_VALUE'].dropna().tolist()
            latest = float(values[-1]) if values else None

            if latest is None:
                return None

            period = df.iloc[-1]['TIME_PERIOD'] if 'TIME_PERIOD' in df.columns else None

            result = {
                'value': round(latest, 2),
                'period': period,
                'history': [round(v, 2) for v in values[-6:]],
                'source': 'ecb-policy-rate'
            }

            self._set_cache(cache_key, result)
            return result

        except Exception as e:
            print(f"  [ECB] Policy rate error: {e}")
            return None


# Singleton instance
_client_instance = None


def get_ecb_client() -> ECBClient:
    """Get singleton ECBClient instance"""
    global _client_instance
    if _client_instance is None:
        _client_instance = ECBClient()
    return _client_instance


# Test
if __name__ == "__main__":
    print("=" * 50)
    print("ECB Client Test")
    print("=" * 50)

    client = get_ecb_client()

    # Test HICP
    print("\n--- Eurozone HICP ---")
    hicp = client.get_eurozone_hicp()
    if hicp:
        print(f"  Value: {hicp['value']}%")
        print(f"  Period: {hicp['period']}")
        print(f"  History: {hicp['history']}")
    else:
        print("  No data")

    # Test Core HICP
    print("\n--- Eurozone Core HICP ---")
    core = client.get_eurozone_core_hicp()
    if core:
        print(f"  Value: {core['value']}%")
        print(f"  Period: {core['period']}")
    else:
        print("  No data")

    # Test Policy Rate
    print("\n--- ECB Policy Rate ---")
    rate = client.get_policy_rate()
    if rate:
        print(f"  Value: {rate['value']}%")
        print(f"  Period: {rate['period']}")
    else:
        print("  No data")
