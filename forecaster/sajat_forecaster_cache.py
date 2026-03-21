"""
SAJÁT FORECASTER Cache Wrapper

Egyszerű API a ComprehensiveForecaster eredményeinek lekérdezéséhez.
- Cache-eli a forecast eredményeket
- Támogatja az éves, negyedéves és havi forecastokat
- MONOPOL: Csak mi tudunk negyedéves/havi forecastot adni!

Usage:
    from data_sources.nexonomics.sajat_forecaster_cache import get_sajat_forecast

    # Éves forecast
    gdp = get_sajat_forecast('HU', 'gdp', 2026)

    # Negyedéves forecast
    gdp_q1 = get_sajat_forecast('HU', 'gdp', 2026, quarter=1)

    # Havi forecast
    infl_jan = get_sajat_forecast('HU', 'inflation', 2026, month=1)
"""

import os
import json
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path

# Cache file location
CACHE_DIR = Path(__file__).parent / ".forecast_cache"
CACHE_FILE = CACHE_DIR / "sajat_forecasts.json"

# Singleton cache
_forecast_cache: Dict[str, Any] = {}
_cache_loaded = False
_forecaster_available = False

def _ensure_cache_dir():
    """Create cache directory if it doesn't exist."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _load_cache() -> Dict[str, Any]:
    """Load forecast cache from file."""
    global _forecast_cache, _cache_loaded

    if _cache_loaded:
        return _forecast_cache

    _ensure_cache_dir()

    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r') as f:
                _forecast_cache = json.load(f)
            print(f"[SajatForecaster] Loaded {len(_forecast_cache.get('countries', {}))} countries from cache")
        except Exception as e:
            print(f"[SajatForecaster] Cache load error: {e}")
            _forecast_cache = {}

    _cache_loaded = True
    return _forecast_cache

def _save_cache(cache: Dict[str, Any]):
    """Save forecast cache to file."""
    global _forecast_cache
    _ensure_cache_dir()

    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2, default=str)
        _forecast_cache = cache
        print(f"[SajatForecaster] Saved forecasts for {len(cache.get('countries', {}))} countries")
    except Exception as e:
        print(f"[SajatForecaster] Cache save error: {e}")

def refresh_forecasts(countries: List[str] = None, scenario: str = 'REALISTIC') -> bool:
    """
    Futtatja a SAJÁT FORECASTER-t és cache-eli az eredményeket.

    Args:
        countries: Országok listája (None = összes)
        scenario: 'PESSIMISTIC', 'REALISTIC', 'OPTIMISTIC'

    Returns:
        True ha sikeres
    """
    global _forecaster_available

    try:
        from .comprehensive_forecaster import (
            ComprehensiveConfig,
            ComprehensiveDataFetcher,
            ComprehensiveForecaster
        )
        _forecaster_available = True
    except ImportError as e:
        print(f"[SajatForecaster] Cannot import comprehensive_forecaster: {e}")
        return False

    print(f"\n{'='*60}")
    print("🏆 SAJÁT FORECASTER - Forecast Refresh")
    print(f"{'='*60}")

    try:
        # Initialize config
        config = ComprehensiveConfig()

        # Override countries if specified
        if countries:
            config.COUNTRIES = countries

        # Fetch data (async method - need to run with asyncio)
        print("\n📊 Fetching data from all sources...")
        fetcher = ComprehensiveDataFetcher(config)

        # Run async fetch
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If already in async context, create new loop
                import nest_asyncio
                nest_asyncio.apply()
                all_data = loop.run_until_complete(fetcher.fetch_all_data())
            else:
                all_data = loop.run_until_complete(fetcher.fetch_all_data())
        except RuntimeError:
            # No event loop exists, create one
            all_data = asyncio.run(fetcher.fetch_all_data())

        # Generate forecasts
        print("\n🔮 Generating forecasts...")
        forecaster = ComprehensiveForecaster(config, all_data)
        all_forecasts = forecaster.forecast_all_comprehensive(scenarios=[scenario])

        # Convert to cache format
        cache = {
            'generated_at': datetime.now().isoformat(),
            'scenario': scenario,
            'countries': {}
        }

        for country, country_data in all_forecasts.items():
            if scenario not in country_data:
                continue

            scenario_data = country_data[scenario]
            country_cache = {
                'gdp': {},
                'inflation': {},
                'unemployment': {}
            }

            # Process GDP forecast
            if 'gdp' in scenario_data and not scenario_data['gdp'].empty:
                gdp_df = scenario_data['gdp']
                for idx, row in gdp_df.iterrows():
                    period = str(idx)  # e.g., "2026-Q1"
                    country_cache['gdp'][period] = {
                        'value': float(row.get('GDP', 0)),
                        'growth_qoq': float(row.get('growth_qoq', 0)),
                        'growth_yoy': float(row.get('growth_yoy', 0))
                    }

            # Process inflation forecast
            if 'inflation' in scenario_data and not scenario_data['inflation'].empty:
                infl_df = scenario_data['inflation']
                for idx, row in infl_df.iterrows():
                    period = str(idx)
                    country_cache['inflation'][period] = {
                        'value': float(row.get('inflation', 0)),
                        'change': float(row.get('change', 0)) if 'change' in row else 0
                    }

            # Process unemployment forecast
            if 'unemployment' in scenario_data and not scenario_data['unemployment'].empty:
                unemp_df = scenario_data['unemployment']
                for idx, row in unemp_df.iterrows():
                    # Unemployment DataFrame may have year/quarter columns instead of datetime index
                    if 'year' in row and 'quarter' in row:
                        year = int(row['year'])
                        # Quarter may be 'Q1', 'Q2', etc or just 1, 2, etc
                        quarter_val = row['quarter']
                        if isinstance(quarter_val, str) and quarter_val.startswith('Q'):
                            quarter = int(quarter_val[1])
                        else:
                            quarter = int(quarter_val)
                        quarter_end_dates = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}
                        period = quarter_end_dates.get(quarter, f"{year}-03-31")
                    else:
                        period = str(idx)
                    country_cache['unemployment'][period] = {
                        'value': float(row.get('unemployment', 0)),
                        'change': float(row.get('unemployment_change', 0)) if 'unemployment_change' in row else 0
                    }

            cache['countries'][country] = country_cache

        # Save cache
        _save_cache(cache)

        print(f"\n✅ Forecasts cached for {len(cache['countries'])} countries")
        return True

    except Exception as e:
        print(f"[SajatForecaster] Forecast refresh error: {e}")
        import traceback
        traceback.print_exc()
        return False

def _parse_cache_key_to_quarter(key: str) -> Optional[tuple]:
    """Parse cache key like '2026-03-31 00:00:00' to (year, quarter)."""
    try:
        # Handle datetime string format
        if ' ' in key:
            date_part = key.split(' ')[0]
        else:
            date_part = key

        parts = date_part.split('-')
        if len(parts) >= 2:
            year = int(parts[0])
            month = int(parts[1])
            quarter = (month - 1) // 3 + 1
            return (year, quarter)
    except:
        pass
    return None

def _get_quarter_end_date(year: int, quarter: int) -> str:
    """Get quarter end date string for matching cache keys."""
    quarter_ends = {
        1: f"{year}-03-31",
        2: f"{year}-06-30",
        3: f"{year}-09-30",
        4: f"{year}-12-31"
    }
    return quarter_ends.get(quarter, f"{year}-03-31")

def get_sajat_forecast(
    country: str,
    indicator: str,  # 'gdp', 'inflation', 'unemployment'
    year: int,
    quarter: Optional[int] = None,
    month: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    Lekérdezi a SAJÁT FORECASTER előrejelzését.

    Args:
        country: ISO 2 country code (e.g., 'HU', 'DE', 'US')
        indicator: 'gdp', 'inflation', 'unemployment'
        year: Target year (e.g., 2026)
        quarter: 1-4 for quarterly forecast
        month: 1-12 for monthly forecast

    Returns:
        {
            'value': float,  # The forecasted value
            'growth': float, # Growth rate (for GDP)
            'period': str,   # e.g., "2026-Q1" or "2026" or "2026-01"
            'frequency': str, # 'annual', 'quarterly', 'monthly'
            'source': 'sajat_forecaster'
        }
    """
    cache = _load_cache()

    if not cache or 'countries' not in cache:
        return None

    country = country.upper()
    if country not in cache['countries']:
        return None

    country_data = cache['countries'][country]
    if indicator not in country_data:
        return None

    indicator_data = country_data[indicator]
    if not indicator_data:
        return None

    # Helper to find value by year and quarter
    def find_value(target_year: int, target_quarter: int) -> Optional[Dict]:
        # Try matching by date string pattern
        for key, val in indicator_data.items():
            parsed = _parse_cache_key_to_quarter(key)
            if parsed and parsed[0] == target_year and parsed[1] == target_quarter:
                return val
        return None

    # Build period key based on frequency
    if month is not None:
        # Monthly forecast - interpolate from quarterly
        frequency = 'monthly'
        quarter_for_month = (month - 1) // 3 + 1

        val = find_value(year, quarter_for_month)
        if val:
            return {
                'value': val.get('growth_yoy', 0) if indicator == 'gdp' else val.get('value', 0),
                'growth': val.get('growth_yoy', 0) if indicator == 'gdp' else 0,
                'period': f"{year}-{month:02d}",
                'frequency': frequency,
                'source': 'sajat_forecaster',
                'note': 'Interpolated from quarterly forecast'
            }

    elif quarter is not None:
        # Quarterly forecast
        frequency = 'quarterly'
        val = find_value(year, quarter)
        if val:
            return {
                'value': val.get('growth_yoy', 0) if indicator == 'gdp' else val.get('value', 0),
                'growth': val.get('growth_yoy', 0) if indicator == 'gdp' else 0,
                'period': f"{year}-Q{quarter}",
                'frequency': frequency,
                'source': 'sajat_forecaster'
            }

    else:
        # Annual forecast - average of all quarters
        frequency = 'annual'
        values = []
        growths = []

        for q in range(1, 5):
            val = find_value(year, q)
            if val:
                if indicator == 'gdp':
                    growths.append(val.get('growth_yoy', 0))
                else:
                    values.append(val.get('value', 0))

        if growths:  # GDP - average growth
            return {
                'value': sum(growths) / len(growths),
                'growth': sum(growths) / len(growths),
                'period': str(year),
                'frequency': frequency,
                'source': 'sajat_forecaster'
            }
        elif values:  # Inflation/Unemployment - average value
            return {
                'value': sum(values) / len(values),
                'growth': 0,
                'period': str(year),
                'frequency': frequency,
                'source': 'sajat_forecaster'
            }

    return None

def get_combined_forecast(
    country: str,
    indicator: str,
    year: int,
    imf_forecast: Optional[float] = None,
    quarter: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    Kombinált forecast: IMF WEO + SAJÁT átlaga.

    A backtesting alapján:
    - GDP: SAJÁT jobb (MAE 0.78% vs IMF 0.81%)
    - Inflation: IMF jobb (MAE 0.34% vs SAJÁT 0.53%)
    - Combined: IMF kicsit jobb (0.57% vs 0.65%)

    Stratégia:
    - Ha mindkettő elérhető: átlag
    - Ha csak egyik: azt használjuk
    - GDP-nél SAJÁT 55%, IMF 45% súlyozás
    - Inflációnál IMF 60%, SAJÁT 40% súlyozás

    Args:
        country: ISO 2 country code
        indicator: 'gdp' or 'inflation'
        year: Target year
        imf_forecast: IMF WEO forecast value (if available)
        quarter: Optional quarter for quarterly forecast

    Returns:
        {
            'combined_forecast': float,
            'sajat_forecast': float,
            'imf_forecast': float,
            'weights': {'sajat': float, 'imf': float},
            'confidence': int,
            'source': 'combined_imf_sajat'
        }
    """
    sajat = get_sajat_forecast(country, indicator, year, quarter=quarter)

    sajat_val = None
    if sajat:
        sajat_val = sajat.get('value') or sajat.get('growth')

    # Súlyozás a backtesting eredmények alapján
    if indicator == 'gdp':
        # GDP: SAJÁT jobb, nagyobb súly
        sajat_weight = 0.55
        imf_weight = 0.45
    elif indicator == 'unemployment':
        # Unemployment: SAJÁT használja Okun's Law + Phillips Curve
        # IMF éves adatokat ad, SAJÁT negyedéves - mindkettő hasznos
        sajat_weight = 0.50
        imf_weight = 0.50
    else:
        # Inflation: IMF jobb, nagyobb súly
        sajat_weight = 0.40
        imf_weight = 0.60

    # Calculate combined forecast
    if sajat_val is not None and imf_forecast is not None:
        combined = sajat_val * sajat_weight + imf_forecast * imf_weight
        confidence = 75  # Magas bizalom, mert mindkét forrás elérhető
        source = 'combined_imf_sajat'
    elif sajat_val is not None:
        combined = sajat_val
        confidence = 65 if quarter else 60  # SAJÁT egyedül
        source = 'sajat_only'
        # MONOPOL: Negyedéves/havi forecastnál csak mi vagyunk!
        if quarter:
            confidence = 70  # Magasabb, mert ez egyedi képesség
    elif imf_forecast is not None:
        combined = imf_forecast
        confidence = 65  # IMF egyedül
        source = 'imf_only'
    else:
        return None

    return {
        'combined_forecast': combined,
        'sajat_forecast': sajat_val,
        'imf_forecast': imf_forecast,
        'weights': {'sajat': sajat_weight, 'imf': imf_weight},
        'confidence': confidence,
        'source': source,
        'is_quarterly': quarter is not None,
        'note': 'MONOPOL: Quarterly forecasting' if quarter and source == 'sajat_only' else None
    }

def is_forecaster_available() -> bool:
    """Check if the SAJÁT FORECASTER is available."""
    try:
        from .comprehensive_forecaster import ComprehensiveForecaster
        return True
    except ImportError:
        return False

def get_cache_info() -> Dict[str, Any]:
    """Get information about the forecast cache."""
    cache = _load_cache()

    if not cache:
        return {'status': 'empty', 'countries': 0}

    return {
        'status': 'loaded',
        'generated_at': cache.get('generated_at'),
        'scenario': cache.get('scenario'),
        'countries': len(cache.get('countries', {})),
        'country_list': list(cache.get('countries', {}).keys())
    }


# =============================================================================
# NOWCAST ADJUSTMENT - Friss adatokkal való korrekció
# =============================================================================

def get_nowcast_adjusted_forecast(
    country: str,
    indicator: str,
    year: int,
    quarter: Optional[int] = None,
    month: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    SAJÁT forecast + nowcast adjustment friss adatok alapján.

    Ha van frissebb aktuális adat, mint amit a cache tartalmaz,
    a forecast-ot korrigáljuk a deviation alapján.

    Adjustment formula:
        adjusted = baseline + (actual - expected) * decay_factor

    Decay factor csökken az idővel (frissebb adat = nagyobb hatás)
    """
    from datetime import datetime

    # 1. Get baseline SAJÁT forecast
    baseline = get_sajat_forecast(country, indicator, year, quarter=quarter, month=month)
    if not baseline:
        return None

    baseline_value = baseline.get('value', 0)

    # 2. Try to get latest actual data from MonthlyForecaster
    try:
        from .monthly_forecaster import MonthlyForecaster
        mf = MonthlyForecaster()

        actual_data = None
        actual_month = None

        # Map country to MonthlyForecaster method
        country_upper = country.upper()

        if indicator == 'inflation':
            if country_upper == 'US':
                fc = mf.forecast_us_cpi()
                actual_data = fc.last_actual if fc else None
                actual_month = fc.last_actual_month if fc else None
            elif country_upper in ['EA', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'AT']:
                fc = mf.forecast_eurozone_hicp()
                actual_data = fc.last_actual if fc else None
                actual_month = fc.last_actual_month if fc else None
            elif country_upper == 'CN':
                fc = mf.forecast_china_cpi()
                actual_data = fc.last_actual if fc else None
                actual_month = fc.last_actual_month if fc else None
            elif country_upper == 'BR':
                fc = mf.forecast_brazil_cpi()
                actual_data = fc.last_actual if fc else None
                actual_month = fc.last_actual_month if fc else None
            elif country_upper == 'MX':
                fc = mf.forecast_mexico_cpi()
                actual_data = fc.last_actual if fc else None
                actual_month = fc.last_actual_month if fc else None
            elif country_upper == 'IN':
                fc = mf.forecast_india_cpi()
                actual_data = fc.last_actual if fc else None
                actual_month = fc.last_actual_month if fc else None

        elif indicator == 'unemployment':
            if country_upper == 'US':
                fc = mf.forecast_us_unemployment()
                actual_data = fc.last_actual if fc else None
                actual_month = fc.last_actual_month if fc else None
            elif country_upper in ['EA', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'AT']:
                fc = mf.forecast_eurozone_unemployment()
                actual_data = fc.last_actual if fc else None
                actual_month = fc.last_actual_month if fc else None

        # 3. Calculate adjustment if we have actual data
        if actual_data is not None and actual_month:
            # Parse actual month
            try:
                actual_date = datetime.strptime(actual_month, '%Y-%m')
            except:
                actual_date = None

            if actual_date:
                # Calculate how many months until target
                target_quarter = quarter or ((month - 1) // 3 + 1 if month else 2)
                target_month_num = month or (target_quarter * 3)
                target_date = datetime(year, target_month_num, 1)

                months_ahead = (target_date.year - actual_date.year) * 12 + (target_date.month - actual_date.month)

                if months_ahead > 0:
                    # Calculate expected value for actual month from baseline
                    # Simple interpolation: assume linear path
                    expected_at_actual = baseline_value  # Simplified

                    # Deviation
                    deviation = actual_data - expected_at_actual

                    # Decay factor: closer = more impact
                    # After 12 months, impact is ~37% (e^-1)
                    decay_factor = 0.8 ** (months_ahead / 3)  # Quarterly decay

                    # Adjust
                    adjustment = deviation * decay_factor
                    adjusted_value = baseline_value + adjustment

                    return {
                        'value': adjusted_value,
                        'baseline_value': baseline_value,
                        'adjustment': adjustment,
                        'last_actual': actual_data,
                        'last_actual_month': actual_month,
                        'decay_factor': decay_factor,
                        'months_ahead': months_ahead,
                        'period': baseline.get('period'),
                        'frequency': baseline.get('frequency'),
                        'source': 'sajat_nowcast_adjusted',
                        'note': f'Adjusted by {adjustment:+.2f}pp based on {actual_month} actual ({actual_data:.2f}%)'
                    }

    except ImportError:
        pass
    except Exception as e:
        print(f"[Nowcast] Adjustment error for {country}/{indicator}: {e}")

    # 4. Return baseline if no adjustment possible
    baseline['source'] = 'sajat_baseline'
    return baseline


def get_smart_forecast(
    country: str,
    indicator: str,
    year: int,
    quarter: Optional[int] = None,
    month: Optional[int] = None,
    imf_forecast: Optional[float] = None
) -> Optional[Dict[str, Any]]:
    """
    ULTIMATE SMART FORECAST - Kombinálja:
    1. SAJÁT baseline forecast (cache)
    2. Nowcast adjustment (friss aktual adatok)
    3. IMF forecast (ha elérhető)

    Ez a végső, production forecast!
    """
    # 1. Get nowcast-adjusted SAJÁT forecast
    nowcast = get_nowcast_adjusted_forecast(country, indicator, year, quarter=quarter, month=month)

    if not nowcast:
        # Fallback to basic combined
        return get_combined_forecast(country, indicator, year, imf_forecast=imf_forecast, quarter=quarter)

    nowcast_value = nowcast.get('value', 0)

    # 2. Blend with IMF if available
    if imf_forecast is not None:
        # Súlyozás: SAJÁT nowcast gets more weight because it's adjusted
        if indicator == 'gdp':
            nowcast_weight = 0.60  # SAJÁT nowcast dominates
            imf_weight = 0.40
        elif indicator == 'unemployment':
            nowcast_weight = 0.55
            imf_weight = 0.45
        else:  # inflation
            nowcast_weight = 0.50  # More balanced for inflation
            imf_weight = 0.50

        combined_value = nowcast_value * nowcast_weight + imf_forecast * imf_weight

        return {
            'combined_forecast': combined_value,
            'nowcast_forecast': nowcast_value,
            'imf_forecast': imf_forecast,
            'baseline_forecast': nowcast.get('baseline_value'),
            'adjustment': nowcast.get('adjustment', 0),
            'last_actual': nowcast.get('last_actual'),
            'last_actual_month': nowcast.get('last_actual_month'),
            'weights': {'nowcast': nowcast_weight, 'imf': imf_weight},
            'confidence': 80,  # High confidence with both sources
            'source': 'smart_combined_nowcast',
            'is_quarterly': quarter is not None
        }

    # 3. Return nowcast-only
    return {
        'combined_forecast': nowcast_value,
        'nowcast_forecast': nowcast_value,
        'baseline_forecast': nowcast.get('baseline_value'),
        'adjustment': nowcast.get('adjustment', 0),
        'last_actual': nowcast.get('last_actual'),
        'last_actual_month': nowcast.get('last_actual_month'),
        'confidence': 70,
        'source': nowcast.get('source', 'sajat_nowcast'),
        'is_quarterly': quarter is not None
    }


# Quick test
if __name__ == "__main__":
    print("=" * 60)
    print("SAJÁT FORECASTER CACHE TEST")
    print("=" * 60)

    # Check availability
    print(f"\nForecaster available: {is_forecaster_available()}")

    # Check cache
    info = get_cache_info()
    print(f"\nCache info: {info}")

    if info['countries'] == 0:
        print("\n⚠️ Cache is empty. Run refresh_forecasts() to populate.")
        print("\nExample:")
        print("  from sajat_forecaster_cache import refresh_forecasts")
        print("  refresh_forecasts(['HU', 'DE', 'US'])  # Quick test with 3 countries")
    else:
        # Test queries
        print("\n📊 Test queries:")

        for country in ['HU', 'DE', 'US'][:3]:
            if country in info.get('country_list', []):
                gdp = get_sajat_forecast(country, 'gdp', 2026)
                if gdp:
                    print(f"  {country} GDP 2026: {gdp['value']:.2f}% ({gdp['frequency']})")

                infl = get_sajat_forecast(country, 'inflation', 2026)
                if infl:
                    print(f"  {country} Inflation 2026: {infl['value']:.2f}%")

                # Test quarterly
                gdp_q1 = get_sajat_forecast(country, 'gdp', 2026, quarter=1)
                if gdp_q1:
                    print(f"  {country} GDP 2026-Q1: {gdp_q1['value']:.2f}% (QUARTERLY!)")
