"""
IMF Direct API Client - Scrapes data directly from IMF DataMapper API

Provides real-time access to:
- NGDP_RPCH: Real GDP Growth (annual % change)
- PCPIPCH: Inflation (annual % change)
- LUR: Unemployment rate

This is a FALLBACK for when DBnomics data is outdated.
IMF updates this data more frequently than DBnomics mirrors it.

API: https://www.imf.org/external/datamapper/api/v1/{indicator}
"""

import requests
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import json

logger = logging.getLogger(__name__)

# Cache for IMF data (to avoid hammering the API)
_imf_cache: Dict[str, Dict] = {}
_cache_timestamp: Optional[datetime] = None
CACHE_TTL_HOURS = 6  # Refresh every 6 hours


class IMFDirectClient:
    """
    Direct client for IMF DataMapper API.

    Provides fresher data than DBnomics for GDP forecasts.

    Usage:
        client = IMFDirectClient()
        gdp = client.get_gdp_forecast('USA', 2026)  # Returns 2.1%
    """

    BASE_URL = "https://www.imf.org/external/datamapper/api/v1"

    # Indicator codes
    INDICATORS = {
        'gdp': 'NGDP_RPCH',      # Real GDP growth
        'inflation': 'PCPIPCH',   # Inflation rate
        'unemployment': 'LUR',    # Unemployment rate
    }

    # Country code mapping (IMF uses ISO3)
    COUNTRY_MAP = {
        # Major economies
        'USA': 'USA', 'US': 'USA',
        'DEU': 'DEU', 'GERMANY': 'DEU', 'DE': 'DEU',
        'GBR': 'GBR', 'UK': 'GBR', 'GB': 'GBR',
        'FRA': 'FRA', 'FRANCE': 'FRA', 'FR': 'FRA',
        'JPN': 'JPN', 'JAPAN': 'JPN', 'JP': 'JPN',
        'CHN': 'CHN', 'CHINA': 'CHN', 'CN': 'CHN',
        'IND': 'IND', 'INDIA': 'IND', 'IN': 'IND',
        'CAN': 'CAN', 'CANADA': 'CAN', 'CA': 'CAN',
        'AUS': 'AUS', 'AUSTRALIA': 'AUS', 'AU': 'AUS',
        'BRA': 'BRA', 'BRAZIL': 'BRA', 'BR': 'BRA',
        'MEX': 'MEX', 'MEXICO': 'MEX', 'MX': 'MEX',
        'KOR': 'KOR', 'KOREA': 'KOR', 'KR': 'KOR',

        # Europe
        'ITA': 'ITA', 'ITALY': 'ITA', 'IT': 'ITA',
        'ESP': 'ESP', 'SPAIN': 'ESP', 'ES': 'ESP',
        'NLD': 'NLD', 'NETHERLANDS': 'NLD', 'NL': 'NLD',
        'BEL': 'BEL', 'BELGIUM': 'BEL', 'BE': 'BEL',
        'CHE': 'CHE', 'SWITZERLAND': 'CHE', 'CH': 'CHE',
        'AUT': 'AUT', 'AUSTRIA': 'AUT', 'AT': 'AUT',
        'POL': 'POL', 'POLAND': 'POL', 'PL': 'POL',
        'SWE': 'SWE', 'SWEDEN': 'SWE', 'SE': 'SWE',
        'NOR': 'NOR', 'NORWAY': 'NOR', 'NO': 'NOR',
        'DNK': 'DNK', 'DENMARK': 'DNK', 'DK': 'DNK',
        'FIN': 'FIN', 'FINLAND': 'FIN', 'FI': 'FIN',
        'IRL': 'IRL', 'IRELAND': 'IRL', 'IE': 'IRL',
        'PRT': 'PRT', 'PORTUGAL': 'PRT', 'PT': 'PRT',
        'GRC': 'GRC', 'GREECE': 'GRC', 'GR': 'GRC',
        'CZE': 'CZE', 'CZECHIA': 'CZE', 'CZ': 'CZE',
        'HUN': 'HUN', 'HUNGARY': 'HUN', 'HU': 'HUN',
        'ROU': 'ROU', 'ROMANIA': 'ROU', 'RO': 'ROU',
        'BGR': 'BGR', 'BULGARIA': 'BGR', 'BG': 'BGR',
        'SVK': 'SVK', 'SLOVAKIA': 'SVK', 'SK': 'SVK',
        'LUX': 'LUX', 'LUXEMBOURG': 'LUX', 'LU': 'LUX',
        'HRV': 'HRV', 'CROATIA': 'HRV', 'HR': 'HRV',
        'SVN': 'SVN', 'SLOVENIA': 'SVN', 'SI': 'SVN',
        'EST': 'EST', 'ESTONIA': 'EST', 'EE': 'EST',
        'LVA': 'LVA', 'LATVIA': 'LVA', 'LV': 'LVA',
        'LTU': 'LTU', 'LITHUANIA': 'LTU', 'LT': 'LTU',

        # Others
        'TUR': 'TUR', 'TURKEY': 'TUR', 'TR': 'TUR',
        'RUS': 'RUS', 'RUSSIA': 'RUS', 'RU': 'RUS',
        'SAU': 'SAU', 'SAUDI': 'SAU', 'SA': 'SAU',
        'ARE': 'ARE', 'UAE': 'ARE', 'AE': 'ARE',
        'ZAF': 'ZAF', 'SOUTH AFRICA': 'ZAF', 'ZA': 'ZAF',
        'EGY': 'EGY', 'EGYPT': 'EGY', 'EG': 'EGY',
        'NGA': 'NGA', 'NIGERIA': 'NGA', 'NG': 'NGA',
        'ARG': 'ARG', 'ARGENTINA': 'ARG', 'AR': 'ARG',
        'CHL': 'CHL', 'CHILE': 'CHL', 'CL': 'CHL',
        'COL': 'COL', 'COLOMBIA': 'COL', 'CO': 'COL',
        'IDN': 'IDN', 'INDONESIA': 'IDN', 'ID': 'IDN',
        'MYS': 'MYS', 'MALAYSIA': 'MYS', 'MY': 'MYS',
        'THA': 'THA', 'THAILAND': 'THA', 'TH': 'THA',
        'SGP': 'SGP', 'SINGAPORE': 'SGP', 'SG': 'SGP',
        'PHL': 'PHL', 'PHILIPPINES': 'PHL', 'PH': 'PHL',
        'VNM': 'VNM', 'VIETNAM': 'VNM', 'VN': 'VNM',

        # Eurozone aggregate
        'EURO': 'EURO', 'EUROZONE': 'EURO', 'EA': 'EURO', 'EUR': 'EURO',
    }

    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'OpScanner/1.0 (Economic Forecast Tool)',
            'Accept': 'application/json',
        })

    def _normalize_country(self, country: str) -> Optional[str]:
        """Convert country input to IMF country code."""
        key = country.upper().strip()
        return self.COUNTRY_MAP.get(key)

    def _get_all_data(self, indicator: str) -> Optional[Dict]:
        """Fetch all country data for an indicator (cached)."""
        global _imf_cache, _cache_timestamp

        cache_key = indicator
        now = datetime.now()

        # Check cache freshness
        if _cache_timestamp and (now - _cache_timestamp).total_seconds() < CACHE_TTL_HOURS * 3600:
            if cache_key in _imf_cache:
                return _imf_cache[cache_key]

        try:
            url = f"{self.BASE_URL}/{indicator}"
            logger.debug(f"[IMF Direct] Fetching: {url}")

            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()

            data = resp.json()

            # Cache the result
            _imf_cache[cache_key] = data
            _cache_timestamp = now

            return data

        except Exception as e:
            logger.warning(f"[IMF Direct] Failed to fetch {indicator}: {e}")
            return None

    def get_gdp_forecast(self, country: str, year: int = None) -> Optional[Dict[str, Any]]:
        """
        Get GDP growth forecast from IMF DataMapper.

        Args:
            country: Country code or name (e.g., 'USA', 'Germany', 'CAN')
            year: Specific year (default: next year)

        Returns:
            {
                'country': 'USA',
                'indicator': 'gdp',
                'source': 'IMF/DataMapper',
                'forecast': 2.1,  # for target year
                'all_forecasts': {2024: 2.8, 2025: 2.0, 2026: 2.1, ...}
            }
        """
        if year is None:
            year = datetime.now().year + 1

        imf_code = self._normalize_country(country)
        if not imf_code:
            logger.warning(f"[IMF Direct] Unknown country: {country}")
            return None

        data = self._get_all_data(self.INDICATORS['gdp'])
        if not data:
            return None

        try:
            # Navigate JSON structure: values -> {indicator} -> {country} -> {year: value}
            values = data.get('values', {})
            indicator_data = values.get(self.INDICATORS['gdp'], {})
            country_data = indicator_data.get(imf_code, {})

            if not country_data:
                logger.warning(f"[IMF Direct] No data for {imf_code}")
                return None

            # Build forecasts dict
            all_forecasts = {}
            for y, val in country_data.items():
                try:
                    all_forecasts[int(y)] = float(val)
                except (ValueError, TypeError):
                    continue

            target_forecast = all_forecasts.get(year)

            return {
                'country': imf_code,
                'indicator': 'gdp',
                'source': 'IMF/DataMapper',
                'forecast': target_forecast,
                'year': year,
                'all_forecasts': all_forecasts,
            }

        except Exception as e:
            logger.error(f"[IMF Direct] Error parsing GDP data: {e}")
            return None

    def get_inflation_forecast(self, country: str, year: int = None) -> Optional[Dict[str, Any]]:
        """Get inflation forecast from IMF DataMapper."""
        if year is None:
            year = datetime.now().year + 1

        imf_code = self._normalize_country(country)
        if not imf_code:
            return None

        data = self._get_all_data(self.INDICATORS['inflation'])
        if not data:
            return None

        try:
            values = data.get('values', {})
            indicator_data = values.get(self.INDICATORS['inflation'], {})
            country_data = indicator_data.get(imf_code, {})

            if not country_data:
                return None

            all_forecasts = {}
            for y, val in country_data.items():
                try:
                    all_forecasts[int(y)] = float(val)
                except (ValueError, TypeError):
                    continue

            return {
                'country': imf_code,
                'indicator': 'inflation',
                'source': 'IMF/DataMapper',
                'forecast': all_forecasts.get(year),
                'year': year,
                'all_forecasts': all_forecasts,
            }

        except Exception as e:
            logger.error(f"[IMF Direct] Error parsing inflation data: {e}")
            return None


# Convenience function
def get_imf_gdp(country: str, year: int = None) -> Optional[float]:
    """Quick helper to get IMF GDP forecast."""
    client = IMFDirectClient()
    result = client.get_gdp_forecast(country, year)
    return result.get('forecast') if result else None


if __name__ == "__main__":
    # Test the client
    client = IMFDirectClient()

    print("IMF Direct Client Test")
    print("=" * 50)

    test_countries = ['USA', 'DEU', 'CAN', 'GBR', 'JPN', 'CHN', 'HUN']

    for code in test_countries:
        result = client.get_gdp_forecast(code)
        if result:
            forecasts = result.get('all_forecasts', {})
            f24 = forecasts.get(2024, '-')
            f25 = forecasts.get(2025, '-')
            f26 = forecasts.get(2026, '-')
            f27 = forecasts.get(2027, '-')
            print(f"{code}: 2024={f24}%, 2025={f25}%, 2026={f26}%, 2027={f27}%")
        else:
            print(f"{code}: No data")
