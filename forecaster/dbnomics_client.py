"""
DBnomics Client - Synchronous client for DBnomics API

Provides easy access to:
- OECD: GDP, Unemployment, Inflation (monthly/quarterly)
- Eurostat: HICP, GDP, Unemployment (EU countries)
- IMF WEO: Forecasts for GDP, Inflation (valuable for prediction markets!)
- World Bank: Historical data (annual)

Usage:
    from data_sources.nexonomics.dbnomics_client import DBnomicsClient

    client = DBnomicsClient()
    gdp = client.get_gdp_growth("USA")
    forecast = client.get_imf_forecast("USA", "gdp")
"""

import logging
from typing import Optional, Dict, List, Any
from datetime import datetime
import time
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

try:
    import dbnomics
    import pandas as pd
    DBNOMICS_AVAILABLE = True
except ImportError:
    DBNOMICS_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default timeout for dbnomics API calls (seconds)
DBNOMICS_TIMEOUT = 10


class DBnomicsClient:
    """
    Synchronous DBnomics client for economic data.

    Focuses on key indicators for prediction markets:
    - GDP growth (quarterly/annual)
    - Inflation rates (monthly)
    - Unemployment (monthly)
    - IMF forecasts (annual projections)
    """

    # Country mappings - 55+ countries for comprehensive global coverage
    # IMF WEO has forecasts for ALL these countries!
    COUNTRY_CODES = {
        # North America
        'usa': 'USA', 'us': 'USA', 'united states': 'USA', 'american': 'USA',
        'canada': 'CAN', 'canadian': 'CAN',
        'mexico': 'MEX', 'mexican': 'MEX',

        # South America
        'brazil': 'BRA', 'brazilian': 'BRA',
        'argentina': 'ARG', 'argentine': 'ARG', 'argentinian': 'ARG',
        'chile': 'CHL', 'chilean': 'CHL',
        'colombia': 'COL', 'colombian': 'COL',
        'peru': 'PER', 'peruvian': 'PER',
        'venezuela': 'VEN', 'venezuelan': 'VEN',
        'ecuador': 'ECU', 'ecuadorian': 'ECU',

        # Western Europe
        'germany': 'DEU', 'german': 'DEU',
        'france': 'FRA', 'french': 'FRA',
        'uk': 'GBR', 'united kingdom': 'GBR', 'britain': 'GBR', 'british': 'GBR',
        'italy': 'ITA', 'italian': 'ITA',
        'spain': 'ESP', 'spanish': 'ESP',
        'netherlands': 'NLD', 'dutch': 'NLD', 'holland': 'NLD',
        'belgium': 'BEL', 'belgian': 'BEL',
        'switzerland': 'CHE', 'swiss': 'CHE',
        'austria': 'AUT', 'austrian': 'AUT',
        'ireland': 'IRL', 'irish': 'IRL',
        'portugal': 'PRT', 'portuguese': 'PRT',
        'luxembourg': 'LUX',

        # Nordic
        'sweden': 'SWE', 'swedish': 'SWE',
        'norway': 'NOR', 'norwegian': 'NOR',
        'denmark': 'DNK', 'danish': 'DNK',
        'finland': 'FIN', 'finnish': 'FIN',
        'iceland': 'ISL', 'icelandic': 'ISL',

        # Central/Eastern Europe
        'poland': 'POL', 'polish': 'POL',
        'czech': 'CZE', 'czechia': 'CZE', 'czech republic': 'CZE',
        'hungary': 'HUN', 'hungarian': 'HUN',
        'romania': 'ROU', 'romanian': 'ROU',
        'bulgaria': 'BGR', 'bulgarian': 'BGR',
        'croatia': 'HRV', 'croatian': 'HRV',
        'slovakia': 'SVK', 'slovak': 'SVK',
        'slovenia': 'SVN', 'slovenian': 'SVN',
        'estonia': 'EST', 'estonian': 'EST',
        'latvia': 'LVA', 'latvian': 'LVA',
        'lithuania': 'LTU', 'lithuanian': 'LTU',
        'ukraine': 'UKR', 'ukrainian': 'UKR',
        'russia': 'RUS', 'russian': 'RUS',

        # Southern Europe
        'greece': 'GRC', 'greek': 'GRC',
        'cyprus': 'CYP',
        'malta': 'MLT',

        # Eurozone aggregate
        'eurozone': 'EA', 'euro area': 'EA', 'euro zone': 'EA',

        # East Asia
        'japan': 'JPN', 'japanese': 'JPN',
        'china': 'CHN', 'chinese': 'CHN',
        'korea': 'KOR', 'south korea': 'KOR', 'korean': 'KOR',
        'taiwan': 'TWN', 'taiwanese': 'TWN',
        'hong kong': 'HKG',

        # Southeast Asia
        'singapore': 'SGP', 'singaporean': 'SGP',
        'thailand': 'THA', 'thai': 'THA',
        'malaysia': 'MYS', 'malaysian': 'MYS',
        'indonesia': 'IDN', 'indonesian': 'IDN',
        'philippines': 'PHL', 'philippine': 'PHL', 'filipino': 'PHL',
        'vietnam': 'VNM', 'vietnamese': 'VNM',

        # South Asia
        'india': 'IND', 'indian': 'IND',
        'pakistan': 'PAK', 'pakistani': 'PAK',
        'bangladesh': 'BGD', 'bangladeshi': 'BGD',

        # Oceania
        'australia': 'AUS', 'australian': 'AUS',
        'new zealand': 'NZL', 'kiwi': 'NZL',

        # Middle East
        'turkey': 'TUR', 'turkish': 'TUR',
        'saudi arabia': 'SAU', 'saudi': 'SAU',
        'uae': 'ARE', 'united arab emirates': 'ARE', 'emirati': 'ARE',
        'israel': 'ISR', 'israeli': 'ISR',
        'qatar': 'QAT', 'qatari': 'QAT',
        'kuwait': 'KWT', 'kuwaiti': 'KWT',
        'iran': 'IRN', 'iranian': 'IRN',
        'iraq': 'IRQ', 'iraqi': 'IRQ',

        # Africa
        'south africa': 'ZAF', 'south african': 'ZAF',
        'egypt': 'EGY', 'egyptian': 'EGY',
        'nigeria': 'NGA', 'nigerian': 'NGA',
        'morocco': 'MAR', 'moroccan': 'MAR',
        'kenya': 'KEN', 'kenyan': 'KEN',
        'algeria': 'DZA', 'algerian': 'DZA',
        'ethiopia': 'ETH', 'ethiopian': 'ETH',
        'ghana': 'GHA', 'ghanaian': 'GHA',
    }

    # EU countries for Eurostat
    EU_COUNTRIES = {
        'DEU', 'FRA', 'ITA', 'ESP', 'NLD', 'BEL', 'AUT', 'POL',
        'SWE', 'DNK', 'FIN', 'CZE', 'HUN', 'ROU', 'BGR', 'HRV',
        'SVK', 'SVN', 'EST', 'LVA', 'LTU', 'GRC', 'PRT', 'IRL',
        'EA'  # Eurozone aggregate
    }

    def __init__(self, timeout: int = DBNOMICS_TIMEOUT):
        self.enabled = DBNOMICS_AVAILABLE
        self.cache = {}
        self.cache_ttl = 3600 * 6  # 6 hours
        self.timeout = timeout

        if not self.enabled:
            logger.warning("DBnomics package not installed - client disabled")
        else:
            logger.info(f"DBnomics client initialized (timeout: {timeout}s)")

    def _fetch_with_timeout(self, provider_code: str, dataset_code: str, series_code: str):
        """
        Fetch series with timeout to prevent blocking on slow API responses.
        Returns DataFrame or None if timeout/error.
        """
        def fetch():
            return dbnomics.fetch_series(
                provider_code=provider_code,
                dataset_code=dataset_code,
                series_code=series_code
            )

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(fetch)
            try:
                return future.result(timeout=self.timeout)
            except FuturesTimeoutError:
                logger.warning(f"Timeout ({self.timeout}s) for {provider_code}/{dataset_code}/{series_code}")
                return None
            except Exception as e:
                logger.debug(f"Fetch error: {e}")
                return None

    def _get_cached(self, key: str) -> Optional[Any]:
        """Get cached value if still valid."""
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.cache_ttl:
                return value
        return None

    def _set_cache(self, key: str, value: Any):
        """Cache a value."""
        self.cache[key] = (value, time.time())

    def normalize_country(self, text: str) -> Optional[str]:
        """Extract and normalize country code from text."""
        import re
        text_lower = text.lower()
        # Sort by length (longest first) to avoid partial matches
        sorted_items = sorted(self.COUNTRY_CODES.items(), key=lambda x: -len(x[0]))
        for name, code in sorted_items:
            # For 2-char patterns, require word boundaries
            if len(name) <= 2:
                if re.search(rf'\b{name}\b', text_lower):
                    return code
            else:
                if name in text_lower:
                    return code
        return None

    def get_gdp_growth(self, country_code: str) -> Optional[Dict[str, Any]]:
        """
        Get latest GDP growth rate for a country.

        Args:
            country_code: ISO3 code (USA, DEU, etc.)

        Returns:
            {
                'value': float (growth rate %),
                'period': str (e.g., '2024-Q3'),
                'source': str,
                'data_points': int
            }
        """
        if not self.enabled:
            return None

        cache_key = f"gdp_{country_code}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            # Try OECD QNA first (quarterly)
            df = self._fetch_with_timeout('OECD', 'QNA', f'{country_code}.B1_GE.GPSA.Q')

            if df is not None and not df.empty:
                latest = df.iloc[-1]
                result = {
                    'value': float(latest['value']),
                    'period': str(latest.get('period', '')).split()[0],
                    'source': 'OECD/QNA',
                    'data_points': len(df),
                    'country': country_code
                }
                self._set_cache(cache_key, result)
                return result

        except Exception as e:
            logger.debug(f"OECD GDP failed for {country_code}: {e}")

        # Try World Bank (annual)
        try:
            iso2 = country_code[:2] if len(country_code) == 3 else country_code
            df = self._fetch_with_timeout('WB', 'WDI', f'{iso2}-NY.GDP.MKTP.KD.ZG')

            if df is not None and not df.empty:
                latest = df.iloc[-1]
                result = {
                    'value': float(latest['value']),
                    'period': str(latest.get('period', '')).split()[0],
                    'source': 'WorldBank/WDI',
                    'data_points': len(df),
                    'country': country_code
                }
                self._set_cache(cache_key, result)
                return result

        except Exception as e:
            logger.debug(f"World Bank GDP failed for {country_code}: {e}")

        return None

    def get_inflation(self, country_code: str) -> Optional[Dict[str, Any]]:
        """
        Get latest inflation rate for a country.

        Args:
            country_code: ISO3 code

        Returns:
            {'value': float, 'period': str, 'source': str}
        """
        if not self.enabled:
            return None

        cache_key = f"inflation_{country_code}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        # EU countries -> Eurostat HICP
        if country_code in self.EU_COUNTRIES:
            try:
                iso2 = country_code if country_code == 'EA' else country_code[:2]
                df = self._fetch_with_timeout('Eurostat', 'prc_hicp_manr', f'M.RCH_A.CP00.{iso2}')

                if df is not None and not df.empty:
                    latest = df.iloc[-1]
                    result = {
                        'value': float(latest['value']),
                        'period': str(latest.get('period', '')).split()[0],
                        'source': 'Eurostat/HICP',
                        'data_points': len(df),
                        'country': country_code
                    }
                    self._set_cache(cache_key, result)
                    return result

            except Exception as e:
                logger.debug(f"Eurostat HICP failed for {country_code}: {e}")

        # Try OECD (all countries)
        try:
            df = self._fetch_with_timeout('OECD', 'MEI', f'{country_code}.CPALTT01.GY.M')

            if df is not None and not df.empty:
                latest = df.iloc[-1]
                value = float(latest['value'])
                # Filter out index values (should be inflation rate < 30%)
                if value < 30:
                    result = {
                        'value': value,
                        'period': str(latest.get('period', '')).split()[0],
                        'source': 'OECD/MEI',
                        'data_points': len(df),
                        'country': country_code
                    }
                    self._set_cache(cache_key, result)
                    return result

        except Exception as e:
            logger.debug(f"OECD MEI failed for {country_code}: {e}")

        return None

    def get_unemployment(self, country_code: str) -> Optional[Dict[str, Any]]:
        """
        Get latest unemployment rate for a country.

        Args:
            country_code: ISO3 code

        Returns:
            {'value': float, 'period': str, 'source': str}
        """
        if not self.enabled:
            return None

        cache_key = f"unemployment_{country_code}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            # OECD harmonised unemployment rate
            df = self._fetch_with_timeout('OECD', 'DP_LIVE', f'{country_code}.HUR.TOT.PC_LF.M')

            if df is not None and not df.empty:
                latest = df.iloc[-1]
                result = {
                    'value': float(latest['value']),
                    'period': str(latest.get('period', '')).split()[0],
                    'source': 'OECD/DP_LIVE',
                    'data_points': len(df),
                    'country': country_code
                }
                self._set_cache(cache_key, result)
                return result

        except Exception as e:
            logger.debug(f"OECD unemployment failed for {country_code}: {e}")

        return None

    def get_imf_forecast(
        self,
        country_code: str,
        indicator: str = 'gdp',
        year: int = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get IMF World Economic Outlook forecast.

        This is VERY VALUABLE for prediction markets - provides official
        forecasts for future years!

        Args:
            country_code: ISO3 code
            indicator: 'gdp', 'inflation', or 'unemployment'
            year: Target year (default: next year)

        Returns:
            {
                'forecast': float,
                'year': int,
                'source': str,
                'all_forecasts': {year: value, ...}
            }
        """
        if not self.enabled:
            return None

        if year is None:
            year = datetime.now().year + 1

        cache_key = f"imf_{indicator}_{country_code}_{year}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        # Map indicator to IMF series
        imf_series = {
            'gdp': 'NGDP_RPCH',  # Real GDP growth
            'inflation': 'PCPIPCH',  # Consumer price inflation
            'unemployment': 'LUR',  # Unemployment rate
        }

        series_code = imf_series.get(indicator)
        if not series_code:
            return None

        try:
            # Try latest WEO releases (from newest to oldest)
            # IMF releases WEO: April (full) and October (update), plus Jan/Jul updates
            # Jan 2026 WEO shows US 2026 GDP at 2.4% (major upgrade from Oct 2024's 1.74%)
            weo_releases = ['WEO:2026-01', 'WEO:2025-10', 'WEO:2025-04', 'WEO:2024-10', 'WEO:2024-04']
            df = None

            for weo_version in weo_releases:
                try:
                    df = self._fetch_with_timeout('IMF', weo_version, f'{country_code}.{series_code}')
                    if df is not None and not df.empty:
                        logger.debug(f"IMF WEO data from {weo_version}")
                        break
                except:
                    continue

            if df is not None and not df.empty:
                # Extract forecasts
                all_forecasts = {}
                target_forecast = None

                for _, row in df.iterrows():
                    period = str(row.get('period', ''))
                    if period:
                        try:
                            forecast_year = int(period[:4])
                            value = float(row['value'])
                            all_forecasts[forecast_year] = value

                            if forecast_year == year:
                                target_forecast = value
                        except (ValueError, TypeError):
                            continue

                if all_forecasts:
                    result = {
                        'forecast': target_forecast,
                        'year': year,
                        'indicator': indicator,
                        'country': country_code,
                        'source': 'IMF/WEO',
                        'all_forecasts': all_forecasts
                    }
                    self._set_cache(cache_key, result)
                    return result

        except Exception as e:
            logger.debug(f"IMF forecast failed for {country_code}: {e}")

        return None

    def get_all_data(self, country_code: str) -> Dict[str, Any]:
        """
        Get all available economic data for a country.

        Returns combined data from all sources.
        """
        result = {
            'country': country_code,
            'gdp': self.get_gdp_growth(country_code),
            'inflation': self.get_inflation(country_code),
            'unemployment': self.get_unemployment(country_code),
            'imf_gdp_forecast': self.get_imf_forecast(country_code, 'gdp'),
            'imf_inflation_forecast': self.get_imf_forecast(country_code, 'inflation'),
        }
        return result


# Convenience function
def get_dbnomics_client() -> Optional[DBnomicsClient]:
    """Get DBnomics client instance."""
    if not DBNOMICS_AVAILABLE:
        return None
    return DBnomicsClient()


if __name__ == "__main__":
    print("=" * 60)
    print("DBnomics Client Test")
    print("=" * 60)

    client = DBnomicsClient()

    if not client.enabled:
        print("DBnomics not available!")
        exit(1)

    # Test countries
    test_countries = ['USA', 'DEU', 'GBR', 'JPN', 'CHN']

    for country in test_countries:
        print(f"\n--- {country} ---")

        # GDP
        gdp = client.get_gdp_growth(country)
        if gdp:
            print(f"  GDP Growth: {gdp['value']:.2f}% ({gdp['period']}) - {gdp['source']}")
        else:
            print(f"  GDP Growth: N/A")

        # Inflation
        infl = client.get_inflation(country)
        if infl:
            print(f"  Inflation: {infl['value']:.1f}% ({infl['period']}) - {infl['source']}")
        else:
            print(f"  Inflation: N/A")

        # Unemployment
        unemp = client.get_unemployment(country)
        if unemp:
            print(f"  Unemployment: {unemp['value']:.1f}% ({unemp['period']}) - {unemp['source']}")
        else:
            print(f"  Unemployment: N/A")

        # IMF Forecast
        forecast = client.get_imf_forecast(country, 'gdp', 2026)
        if forecast and forecast.get('forecast'):
            print(f"  IMF 2026 GDP Forecast: {forecast['forecast']:.1f}%")
            if forecast.get('all_forecasts'):
                years = sorted(forecast['all_forecasts'].keys())[-3:]
                for y in years:
                    print(f"    {y}: {forecast['all_forecasts'][y]:.1f}%")

    print("\nTest completed!")
