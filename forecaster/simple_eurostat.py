"""
Simple Eurostat API wrapper for OpScanner

Direct API calls without complex async machinery.
"""

import requests
from typing import Optional, Dict, List
from datetime import datetime


class SimpleEurostatClient:
    """Simple synchronous Eurostat API client"""

    BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

    # Country code mappings (2-letter to Eurostat format)
    COUNTRY_CODES = {
        'DEU': 'DE', 'FRA': 'FR', 'ITA': 'IT', 'ESP': 'ES',
        'NLD': 'NL', 'BEL': 'BE', 'AUT': 'AT', 'POL': 'PL',
        'CZE': 'CZ', 'HUN': 'HU', 'ROU': 'RO', 'GRC': 'EL',
        'PRT': 'PT', 'SWE': 'SE', 'DNK': 'DK', 'FIN': 'FI',
        'IRL': 'IE', 'SVK': 'SK', 'SVN': 'SI', 'LTU': 'LT',
        'LVA': 'LV', 'EST': 'EE', 'HRV': 'HR', 'BGR': 'BG',
        'LUX': 'LU', 'MLT': 'MT', 'CYP': 'CY',
    }

    # Valid EU/EEA country codes that Eurostat supports
    VALID_EUROSTAT_COUNTRIES = {
        'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'AT', 'PL', 'CZ', 'HU',
        'RO', 'EL', 'PT', 'SE', 'DK', 'FI', 'IE', 'SK', 'SI', 'LT',
        'LV', 'EE', 'HR', 'BG', 'LU', 'MT', 'CY',
        # EEA + candidate countries
        'NO', 'IS', 'LI', 'CH', 'UK', 'GB', 'TR', 'RS', 'ME', 'MK', 'AL',
        # Euro area aggregate
        'EA', 'EU', 'EU27_2020',
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'OpScanner/1.0'
        })

    def _get_eurostat_code(self, country: str) -> Optional[str]:
        """Convert country code to Eurostat format. Returns None if not a valid Eurostat country."""
        # First check 3-letter code mapping
        code = self.COUNTRY_CODES.get(country.upper(), country.upper())
        # Verify it's a valid Eurostat country
        if code not in self.VALID_EUROSTAT_COUNTRIES:
            return None
        return code

    def is_eurostat_country(self, country_code: str) -> bool:
        """Check if a country is supported by Eurostat"""
        code = self.COUNTRY_CODES.get(country_code.upper(), country_code.upper())
        return code in self.VALID_EUROSTAT_COUNTRIES

    def get_gdp_growth(self, country_code: str, periods: int = 8) -> Optional[Dict]:
        """
        Get quarterly GDP growth rate from Eurostat.

        Dataset: namq_10_gdp
        Returns quarterly % change in chain-linked GDP.
        """
        geo = self._get_eurostat_code(country_code)
        if geo is None:
            # Not a Eurostat country - caller should use FRED, OECD, or other source
            return None

        url = f"{self.BASE_URL}/namq_10_gdp"
        params = {
            'format': 'JSON',
            'lang': 'EN',
            'geo': geo,
            'na_item': 'B1GQ',  # GDP
            's_adj': 'SCA',     # Seasonally and calendar adjusted
            'unit': 'CLV_PCH_PRE',  # Chain-linked % change from previous period
            'freq': 'Q',
            'lastTimePeriod': periods
        }

        try:
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return self._parse_eurostat_response(data, 'GDP Growth')
        except Exception as e:
            print(f"[Eurostat] GDP error for {country_code}: {e}")

        return None

    def get_inflation(self, country_code: str, periods: int = 12) -> Optional[Dict]:
        """
        Get monthly HICP inflation from Eurostat.

        Dataset: prc_hicp_manr
        Returns annual rate of change of HICP.
        """
        geo = self._get_eurostat_code(country_code)
        if geo is None:
            return None

        url = f"{self.BASE_URL}/prc_hicp_manr"
        params = {
            'format': 'JSON',
            'lang': 'EN',
            'geo': geo,
            'coicop': 'CP00',  # All items
            'lastTimePeriod': periods
        }

        try:
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return self._parse_eurostat_response(data, 'Inflation')
        except Exception as e:
            print(f"[Eurostat] Inflation error for {country_code}: {e}")

        return None

    def get_unemployment(self, country_code: str, periods: int = 12) -> Optional[Dict]:
        """
        Get monthly unemployment rate from Eurostat.

        Dataset: une_rt_m
        """
        geo = self._get_eurostat_code(country_code)
        if geo is None:
            return None

        url = f"{self.BASE_URL}/une_rt_m"
        params = {
            'format': 'JSON',
            'lang': 'EN',
            'geo': geo,
            's_adj': 'SA',   # Seasonally adjusted
            'age': 'TOTAL',
            'sex': 'T',
            'unit': 'PC_ACT',  # Percentage of active population
            'lastTimePeriod': periods
        }

        try:
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return self._parse_eurostat_response(data, 'Unemployment')
        except Exception as e:
            print(f"[Eurostat] Unemployment error for {country_code}: {e}")

        return None

    def _parse_eurostat_response(self, data: dict, indicator: str) -> Optional[Dict]:
        """Parse Eurostat JSON response into usable format"""
        try:
            # Get dimension info
            dimension = data.get('dimension', {})
            time_dim = dimension.get('time', {}).get('category', {}).get('index', {})

            # Get values
            values = data.get('value', {})

            if not values or not time_dim:
                return None

            # Build time series (index -> period)
            periods = {v: k for k, v in time_dim.items()}

            # Extract latest value
            latest_idx = max(int(k) for k in values.keys())
            latest_value = values[str(latest_idx)]
            latest_period = periods.get(latest_idx, 'unknown')

            # Build history
            history = []
            for idx, val in values.items():
                period = periods.get(int(idx), '')
                if val is not None:
                    history.append({'period': period, 'value': val})

            # Sort by period
            history.sort(key=lambda x: x['period'], reverse=True)

            return {
                'indicator': indicator,
                'value': latest_value,
                'period': latest_period,
                'history': history[:8],  # Last 8 periods
                'source': 'eurostat'
            }

        except Exception as e:
            print(f"[Eurostat] Parse error: {e}")
            return None


class SimpleFREDClient:
    """Simple FRED API client for US data"""

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    # FRED series IDs
    SERIES = {
        'gdp_growth': 'A191RL1Q225SBEA',  # Real GDP % change
        'inflation': 'CPIAUCSL',           # CPI
        'unemployment': 'UNRATE',          # Unemployment rate
        'fed_rate': 'FEDFUNDS',            # Federal funds rate
    }

    def __init__(self, api_key: str = None):
        import os
        self.api_key = api_key or os.getenv('FRED_API_KEY', '')
        self.session = requests.Session()

    def get_indicator(self, indicator: str, periods: int = 12) -> Optional[Dict]:
        """Get a FRED indicator"""
        series_id = self.SERIES.get(indicator)
        if not series_id:
            return None

        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'sort_order': 'desc',
            'limit': periods
        }

        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                observations = data.get('observations', [])

                if observations:
                    latest = observations[0]
                    history = [
                        {'period': obs['date'], 'value': float(obs['value'])}
                        for obs in observations
                        if obs['value'] != '.'
                    ]

                    return {
                        'indicator': indicator,
                        'value': float(latest['value']) if latest['value'] != '.' else None,
                        'period': latest['date'],
                        'history': history,
                        'source': 'fred'
                    }
        except Exception as e:
            print(f"[FRED] Error for {indicator}: {e}")

        return None


# Create singleton instances
_eurostat = None
_fred = None


def get_eurostat_client() -> SimpleEurostatClient:
    global _eurostat
    if _eurostat is None:
        _eurostat = SimpleEurostatClient()
    return _eurostat


def get_fred_client() -> SimpleFREDClient:
    global _fred
    if _fred is None:
        _fred = SimpleFREDClient()
    return _fred


# Test
if __name__ == "__main__":
    print("Testing Eurostat API...")

    eurostat = get_eurostat_client()

    # Test Germany GDP
    result = eurostat.get_gdp_growth('DEU')
    if result:
        print(f"Germany GDP Growth: {result['value']}% ({result['period']})")
    else:
        print("Germany GDP: Failed")

    # Test Germany Inflation
    result = eurostat.get_inflation('DEU')
    if result:
        print(f"Germany Inflation: {result['value']}% ({result['period']})")
    else:
        print("Germany Inflation: Failed")

    print("\nTesting FRED API...")
    fred = get_fred_client()

    # Test US Unemployment
    result = fred.get_indicator('unemployment')
    if result:
        print(f"US Unemployment: {result['value']}% ({result['period']})")
    else:
        print("US Unemployment: Failed (check FRED_API_KEY)")
