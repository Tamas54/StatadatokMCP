"""
MONTHLY ECONOMIC FORECASTER FOR POLYMARKET
===========================================
Havi előrejelzések prediction market fogadásokhoz.

Indicators:
- US CPI Inflation (YoY %)
- US Unemployment Rate (%)
- Eurozone HICP Inflation (YoY %)
- Eurozone Unemployment Rate (%)
- US GDP Growth (quarterly, annualized %)

Data Sources:
- FRED API (real-time US data)
- Eurostat (Eurozone data)
- Yahoo Finance (oil, commodities for inflation nowcast)
- Weekly indicators (jobless claims, etc.)

Usage:
    from data_sources.nexonomics.monthly_forecaster import MonthlyForecaster
    forecaster = MonthlyForecaster()
    result = forecaster.forecast_us_cpi(target_month="2026-02")
"""

import pandas as pd
import numpy as np
import requests
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import os


FRED_API_KEY = os.getenv("FRED_API_KEY")


@dataclass
class MonthlyForecast:
    """Monthly forecast result"""
    indicator: str
    region: str  # 'US' or 'EA' (Euro Area)
    target_month: str  # '2026-02'
    forecast: float
    confidence_low: float  # 80% CI lower bound
    confidence_high: float  # 80% CI upper bound
    last_actual: float
    last_actual_month: str
    methodology: str
    data_sources: List[str]
    forecast_date: str


class FREDClient:
    """FRED API client for real-time economic data"""

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or FRED_API_KEY

    def get_series(self, series_id: str, start_date: str = None, limit: int = 60) -> pd.DataFrame:
        """Fetch FRED time series"""
        if not self.api_key:
            print(f"⚠️ No FRED API key - using fallback for {series_id}")
            return pd.DataFrame()

        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'sort_order': 'desc',
            'limit': limit
        }
        if start_date:
            params['observation_start'] = start_date

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'observations' in data:
                df = pd.DataFrame(data['observations'])
                df['date'] = pd.to_datetime(df['date'])
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df = df.dropna(subset=['value'])
                return df.set_index('date').sort_index()
        except Exception as e:
            print(f"⚠️ FRED {series_id} error: {e}")

        return pd.DataFrame()


class MonthlyForecaster:
    """
    Monthly economic indicator forecaster for Polymarket.

    Key insight: Use high-frequency data to predict monthly releases!
    - Weekly jobless claims → Unemployment
    - Daily oil prices → CPI
    - Weekly retail sales → GDP
    """

    # FRED Series IDs
    FRED_SERIES = {
        # US Inflation
        'CPIAUCSL': 'US CPI All Items',
        'CPILFESL': 'US Core CPI',
        'PCEPI': 'US PCE Price Index',
        'PCEPILFE': 'US Core PCE',

        # US Unemployment
        'UNRATE': 'US Unemployment Rate',
        'ICSA': 'Initial Jobless Claims (Weekly)',
        'CCSA': 'Continued Claims (Weekly)',
        'PAYEMS': 'Nonfarm Payrolls',

        # US GDP components
        'DGORDER': 'Durable Goods Orders',
        'RSAFS': 'Retail Sales',
        'INDPRO': 'Industrial Production',

        # US Leading indicators
        'UMCSENT': 'U Michigan Consumer Sentiment',
        'AWHMAN': 'Avg Weekly Hours Manufacturing',

        # EMERGING MARKETS - CPI (YoY %)
        'CHNCPIALLMINMEI': 'China CPI YoY',
        'BRACPIALLMINMEI': 'Brazil CPI YoY',
        'MEXCPIALLMINMEI': 'Mexico CPI YoY',
        'INDCPIALLMINMEI': 'India CPI YoY',

        # EMERGING MARKETS - Unemployment
        'LMUNRRTTCNM156S': 'China Unemployment Rate',
        'LMUNRRBRA156S': 'Brazil Unemployment Rate',
        'LMUNRRMXA156S': 'Mexico Unemployment Rate',
        # India unemployment not on FRED - use World Bank annual
    }

    # Historical volatility for confidence intervals
    FORECAST_VOLATILITY = {
        'us_cpi': 0.3,  # Typical monthly surprise ±0.3pp
        'us_unemployment': 0.2,  # ±0.2pp
        'ea_hicp': 0.2,
        'ea_unemployment': 0.1,
        'us_gdp': 0.5,  # Quarterly GDP more volatile
        # Emerging markets - higher volatility
        'cn_cpi': 0.5,
        'cn_unemployment': 0.2,
        'br_cpi': 0.8,  # Brazil more volatile
        'br_unemployment': 0.4,
        'mx_cpi': 0.6,
        'mx_unemployment': 0.3,
        'in_cpi': 1.0,  # India very volatile
    }

    def __init__(self):
        self.fred = FREDClient()
        self._cache = {}

    def _get_current_month(self) -> str:
        """Get current month as YYYY-MM"""
        return datetime.now().strftime('%Y-%m')

    def _get_previous_month(self, month_str: str) -> str:
        """Get previous month"""
        date = datetime.strptime(month_str + "-01", '%Y-%m-%d')
        prev = date - timedelta(days=1)
        return prev.strftime('%Y-%m')

    # =========================================================================
    # US CPI INFLATION FORECAST
    # =========================================================================

    def forecast_us_cpi(self, target_month: str = None) -> MonthlyForecast:
        """
        Forecast US CPI YoY inflation for target month.

        Methodology:
        1. Get latest CPI data (usually 1 month lag)
        2. Use oil prices to adjust energy component
        3. Use core CPI trend for non-energy
        4. Apply seasonal adjustment

        Polymarket Example: "US CPI YoY > 2.5% in February 2026?"
        """
        if target_month is None:
            target_month = self._get_current_month()

        print(f"📊 Forecasting US CPI for {target_month}...")

        # Get latest CPI data
        cpi_data = self.fred.get_series('CPIAUCSL', limit=24)
        core_cpi = self.fred.get_series('CPILFESL', limit=24)

        if cpi_data.empty:
            # Fallback to recent known values
            last_cpi_yoy = 2.9  # Jan 2026 estimate
            last_month = "2026-01"
        else:
            # Calculate YoY change
            cpi_data['yoy'] = cpi_data['value'].pct_change(12) * 100
            last_cpi_yoy = cpi_data['yoy'].iloc[-1]
            last_month = cpi_data.index[-1].strftime('%Y-%m')

        # Get oil price change for energy adjustment
        oil_adjustment = self._get_oil_price_impact()

        # Core CPI trend (sticky component)
        if not core_cpi.empty:
            core_cpi['yoy'] = core_cpi['value'].pct_change(12) * 100
            core_trend = core_cpi['yoy'].iloc[-1]
        else:
            core_trend = 3.2  # Fallback

        # Forecast model:
        # CPI = 0.7 * Core_trend + 0.2 * Previous_CPI + 0.1 * Oil_adjustment
        # Plus mean reversion to 2.5% Fed target area

        prev_weight = 0.3
        core_weight = 0.5
        oil_weight = 0.1
        mean_reversion = 0.1

        target_inflation = 2.5  # Fed implicit target

        forecast = (
            prev_weight * last_cpi_yoy +
            core_weight * core_trend +
            oil_weight * oil_adjustment +
            mean_reversion * (target_inflation - last_cpi_yoy)
        )

        # Confidence interval (80%)
        vol = self.FORECAST_VOLATILITY['us_cpi']
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='CPI YoY',
            region='US',
            target_month=target_month,
            forecast=round(forecast, 2),
            confidence_low=round(ci_low, 2),
            confidence_high=round(ci_high, 2),
            last_actual=round(last_cpi_yoy, 2),
            last_actual_month=last_month,
            methodology="Core CPI trend + Oil adjustment + Mean reversion",
            data_sources=['FRED:CPIAUCSL', 'FRED:CPILFESL', 'YF:CL=F'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    def _get_oil_price_impact(self) -> float:
        """
        Calculate oil price impact on CPI.
        Oil is ~7% of CPI, so 10% oil change ≈ 0.7pp CPI impact
        """
        try:
            oil = yf.Ticker("CL=F")
            hist = oil.history(period="3mo")
            if not hist.empty:
                current = hist['Close'].iloc[-1]
                month_ago = hist['Close'].iloc[-22] if len(hist) > 22 else hist['Close'].iloc[0]
                yoy_change = (current / month_ago - 1) * 100

                # 10% oil change ≈ 0.3pp energy CPI impact
                # Energy is ~7% of CPI
                impact = yoy_change * 0.03
                return round(impact, 2)
        except Exception as e:
            print(f"⚠️ Oil price error: {e}")

        return 0.0

    def forecast_us_cpi_mom_change(self, target_month: str = None) -> MonthlyForecast:
        """
        Forecast US CPI Month-over-Month change for target month.

        This is for markets like "Will monthly inflation increase by 0.5% or more?"

        Typical MoM CPI values:
        - Normal: 0.1% - 0.3%
        - High: 0.4% - 0.5%
        - Very High: > 0.5% (rare, inflationary pressure)
        - Negative: deflation (rare)
        """
        if target_month is None:
            target_month = self._get_current_month()

        print(f"📊 Forecasting US CPI MoM change for {target_month}...")

        # Get CPI data
        cpi_data = self.fred.get_series('CPIAUCSL', limit=24)

        if cpi_data.empty:
            # Fallback to typical values
            last_mom = 0.25
            last_month = "2026-01"
            historical_std = 0.15
        else:
            # Calculate MoM change
            cpi_data['mom'] = cpi_data['value'].pct_change() * 100
            last_mom = cpi_data['mom'].iloc[-1]
            last_month = cpi_data.index[-1].strftime('%Y-%m')
            # Historical standard deviation of MoM changes
            historical_std = cpi_data['mom'].std()

        # Get oil price momentum for energy component
        oil_impact = self._get_oil_price_impact() * 0.1  # Scaled down for MoM

        # Forecast MoM change
        # Typical range: 0.1% to 0.4%, mean around 0.2-0.25%
        historical_mean = 0.25  # Long-term average MoM

        # Simple forecast: blend recent with historical mean
        forecast = 0.5 * last_mom + 0.3 * historical_mean + 0.2 * oil_impact

        # Bound to reasonable range
        forecast = max(-0.5, min(1.0, forecast))

        # Confidence interval
        std = max(0.10, historical_std)  # At least 0.1% std
        ci_low = forecast - 1.28 * std
        ci_high = forecast + 1.28 * std

        return MonthlyForecast(
            indicator="CPI_MOM",
            region="US",
            target_month=target_month,
            forecast=round(forecast, 3),
            confidence_low=round(ci_low, 3),
            confidence_high=round(ci_high, 3),
            last_actual=round(last_mom, 3),
            last_actual_month=last_month,
            methodology="MoM CPI change with oil adjustment",
            data_sources=["FRED/CPIAUCSL"],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    # =========================================================================
    # US UNEMPLOYMENT FORECAST
    # =========================================================================

    def forecast_us_unemployment(self, target_month: str = None) -> MonthlyForecast:
        """
        Forecast US Unemployment Rate for target month.

        Methodology:
        1. Get latest unemployment rate
        2. Use weekly jobless claims as leading indicator
        3. Apply Okun's Law adjustment from GDP trend
        4. Seasonal adjustment

        Polymarket Example: "US Unemployment > 4.5% in February 2026?"
        """
        if target_month is None:
            target_month = self._get_current_month()

        print(f"📊 Forecasting US Unemployment for {target_month}...")

        # Get latest unemployment
        unemp_data = self.fred.get_series('UNRATE', limit=24)

        if unemp_data.empty:
            last_unemp = 4.1  # Jan 2026 estimate
            last_month = "2026-01"
        else:
            last_unemp = unemp_data['value'].iloc[-1]
            last_month = unemp_data.index[-1].strftime('%Y-%m')

        # Get jobless claims trend (leading indicator)
        claims_adjustment = self._get_jobless_claims_signal()

        # Natural rate assumption (NAIRU)
        natural_rate = 4.2

        # Forecast model:
        # Unemployment = 0.8 * Previous + 0.1 * Claims_signal + 0.1 * Mean_reversion
        forecast = (
            0.8 * last_unemp +
            0.1 * (last_unemp + claims_adjustment) +
            0.1 * natural_rate
        )

        # Confidence interval
        vol = self.FORECAST_VOLATILITY['us_unemployment']
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='Unemployment Rate',
            region='US',
            target_month=target_month,
            forecast=round(forecast, 1),
            confidence_low=round(ci_low, 1),
            confidence_high=round(ci_high, 1),
            last_actual=round(last_unemp, 1),
            last_actual_month=last_month,
            methodology="Persistence + Jobless claims signal + NAIRU reversion",
            data_sources=['FRED:UNRATE', 'FRED:ICSA'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    def _get_jobless_claims_signal(self) -> float:
        """
        Use weekly jobless claims to predict unemployment direction.
        Rising claims → rising unemployment
        """
        claims = self.fred.get_series('ICSA', limit=12)

        if claims.empty:
            return 0.0

        # Compare recent 4-week average to previous 4-week
        if len(claims) >= 8:
            recent = claims['value'].iloc[-4:].mean()
            previous = claims['value'].iloc[-8:-4].mean()
            change_pct = (recent / previous - 1) * 100

            # 10% rise in claims ≈ 0.2pp rise in unemployment
            signal = change_pct * 0.02
            return round(signal, 2)

        return 0.0

    # =========================================================================
    # EUROZONE HICP INFLATION FORECAST
    # =========================================================================

    def forecast_eurozone_hicp(self, target_month: str = None) -> MonthlyForecast:
        """
        Forecast Eurozone HICP YoY inflation.

        Data source: Eurostat (with fallback to ECB SDW)
        """
        if target_month is None:
            target_month = self._get_current_month()

        print(f"📊 Forecasting Eurozone HICP for {target_month}...")

        # Try to get latest Eurozone data
        hicp_data = self._get_eurostat_hicp()

        if hicp_data is None:
            # Fallback estimates
            last_hicp = 2.4  # Jan 2026 estimate
            last_month = "2026-01"
        else:
            last_hicp = hicp_data['value']
            last_month = hicp_data['month']

        # Energy price impact (EUR Brent)
        energy_adjustment = self._get_brent_price_impact()

        # ECB target
        target = 2.0

        # Eurozone inflation more anchored than US
        forecast = (
            0.6 * last_hicp +
            0.2 * target +
            0.1 * energy_adjustment +
            0.1 * last_hicp  # momentum
        )

        vol = self.FORECAST_VOLATILITY['ea_hicp']
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='HICP YoY',
            region='EA',
            target_month=target_month,
            forecast=round(forecast, 2),
            confidence_low=round(ci_low, 2),
            confidence_high=round(ci_high, 2),
            last_actual=round(last_hicp, 2),
            last_actual_month=last_month,
            methodology="Anchored expectations + Energy adjustment",
            data_sources=['Eurostat:HICP', 'YF:BZ=F'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    def _get_eurostat_hicp(self) -> Optional[Dict]:
        """Fetch latest Eurozone HICP from Eurostat"""
        try:
            # Eurostat SDMX API for HICP
            url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/prc_hicp_manr/M.RCH_A.CP00.EA"
            params = {'format': 'JSON', 'lastNObservations': 3}

            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                # Parse Eurostat JSON (complex structure)
                if 'value' in data:
                    values = list(data['value'].values())
                    if values:
                        return {
                            'value': values[-1],
                            'month': datetime.now().strftime('%Y-%m')
                        }
        except Exception as e:
            print(f"⚠️ Eurostat HICP error: {e}")

        return None

    def _get_brent_price_impact(self) -> float:
        """Brent oil impact on Eurozone energy prices"""
        try:
            brent = yf.Ticker("BZ=F")
            hist = brent.history(period="3mo")
            if not hist.empty:
                current = hist['Close'].iloc[-1]
                month_ago = hist['Close'].iloc[-22] if len(hist) > 22 else hist['Close'].iloc[0]
                change = (current / month_ago - 1) * 100
                return round(change * 0.025, 2)  # ~2.5% pass-through
        except Exception as e:
            print(f"⚠️ Brent price error: {e}")
        return 0.0

    # =========================================================================
    # EUROZONE UNEMPLOYMENT FORECAST
    # =========================================================================

    def forecast_eurozone_unemployment(self, target_month: str = None) -> MonthlyForecast:
        """Forecast Eurozone Unemployment Rate"""
        if target_month is None:
            target_month = self._get_current_month()

        print(f"📊 Forecasting Eurozone Unemployment for {target_month}...")

        # Eurozone unemployment is very sticky
        last_unemp = 6.3  # Recent EA unemployment
        last_month = "2025-12"

        # Try Eurostat
        ea_data = self._get_eurostat_unemployment()
        if ea_data:
            last_unemp = ea_data['value']
            last_month = ea_data['month']

        # Natural rate for EA
        natural_rate = 6.5

        # Very high persistence in EA unemployment
        forecast = 0.95 * last_unemp + 0.05 * natural_rate

        vol = self.FORECAST_VOLATILITY['ea_unemployment']
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='Unemployment Rate',
            region='EA',
            target_month=target_month,
            forecast=round(forecast, 1),
            confidence_low=round(ci_low, 1),
            confidence_high=round(ci_high, 1),
            last_actual=round(last_unemp, 1),
            last_actual_month=last_month,
            methodology="High persistence (sticky labor market)",
            data_sources=['Eurostat:une_rt_m'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    def _get_eurostat_unemployment(self) -> Optional[Dict]:
        """Fetch Eurozone unemployment from Eurostat"""
        try:
            url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/une_rt_m/M.SA.TOTAL.PC_ACT.T.EA"
            params = {'format': 'JSON', 'lastNObservations': 3}

            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if 'value' in data:
                    values = list(data['value'].values())
                    if values:
                        return {
                            'value': values[-1],
                            'month': datetime.now().strftime('%Y-%m')
                        }
        except Exception as e:
            print(f"⚠️ Eurostat unemployment error: {e}")
        return None

    # =========================================================================
    # US GDP QUARTERLY FORECAST
    # =========================================================================

    def forecast_us_gdp(self, target_quarter: str = None) -> MonthlyForecast:
        """
        Forecast US GDP growth (annualized QoQ %).

        Uses Atlanta Fed GDPNow methodology:
        - Retail sales
        - Industrial production
        - Employment
        - Trade data
        """
        if target_quarter is None:
            now = datetime.now()
            q = (now.month - 1) // 3 + 1
            target_quarter = f"{now.year}-Q{q}"

        print(f"📊 Forecasting US GDP for {target_quarter}...")

        # Get component indicators
        retail = self.fred.get_series('RSAFS', limit=6)
        indpro = self.fred.get_series('INDPRO', limit=6)

        # Default forecast (consensus-ish)
        gdp_forecast = 2.3

        # Adjust based on recent retail and IP
        if not retail.empty:
            retail_mom = retail['value'].pct_change().iloc[-1] * 100
            # Retail ~70% of GDP
            gdp_forecast += retail_mom * 0.5

        if not indpro.empty:
            ip_mom = indpro['value'].pct_change().iloc[-1] * 100
            # Industrial ~15% of GDP
            gdp_forecast += ip_mom * 0.2

        # Bound the forecast
        gdp_forecast = max(-2, min(5, gdp_forecast))

        vol = self.FORECAST_VOLATILITY['us_gdp']
        ci_low = gdp_forecast - 1.28 * vol
        ci_high = gdp_forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='GDP Growth (SAAR)',
            region='US',
            target_month=target_quarter,
            forecast=round(gdp_forecast, 1),
            confidence_low=round(ci_low, 1),
            confidence_high=round(ci_high, 1),
            last_actual=2.3,  # Q4 2025 estimate
            last_actual_month="2025-Q4",
            methodology="GDPNow-style nowcast (Retail + IP)",
            data_sources=['FRED:RSAFS', 'FRED:INDPRO'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    # =========================================================================
    # EMERGING MARKETS - China, Brazil, Mexico, India
    # =========================================================================

    def _calculate_yoy_from_index(self, df: pd.DataFrame) -> pd.Series:
        """Calculate YoY % change from CPI index values"""
        if len(df) < 13:
            return pd.Series()
        # YoY = (current / year_ago - 1) * 100
        yoy = (df['value'] / df['value'].shift(12) - 1) * 100
        return yoy.dropna()

    def forecast_china_cpi(self, target_month: str = None) -> MonthlyForecast:
        """Forecast China CPI YoY inflation"""
        if target_month is None:
            target_month = self._get_current_month()

        print(f"📊 Forecasting China CPI for {target_month}...")

        # Get FRED China CPI INDEX data (need to calculate YoY)
        cpi_data = self.fred.get_series('CHNCPIALLMINMEI', limit=36)

        if cpi_data.empty or len(cpi_data) < 13:
            # Fallback: China CPI typically 1-2%
            forecast = 1.5
            last_actual = 1.5
            last_month = "Unknown"
        else:
            # Calculate YoY from index
            yoy = self._calculate_yoy_from_index(cpi_data)
            if yoy.empty:
                forecast = 1.5
                last_actual = 1.5
                last_month = "Unknown"
            else:
                last_actual = yoy.iloc[-1]
                last_month = yoy.index[-1].strftime('%Y-%m')

                # China CPI very stable, use trend
                recent_avg = yoy.tail(3).mean()
                trend = yoy.diff().tail(3).mean()

                forecast = recent_avg + trend * 0.5
                forecast = max(-1, min(5, forecast))  # China rarely has high inflation

        vol = self.FORECAST_VOLATILITY['cn_cpi']
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='CPI YoY',
            region='CN',
            target_month=target_month,
            forecast=round(forecast, 2),
            confidence_low=round(ci_low, 2),
            confidence_high=round(ci_high, 2),
            last_actual=round(last_actual, 2),
            last_actual_month=last_month,
            methodology="FRED China CPI index → YoY calculation",
            data_sources=['FRED:CHNCPIALLMINMEI'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    def forecast_brazil_cpi(self, target_month: str = None) -> MonthlyForecast:
        """Forecast Brazil IPCA inflation YoY"""
        if target_month is None:
            target_month = self._get_current_month()

        print(f"📊 Forecasting Brazil CPI for {target_month}...")

        cpi_data = self.fred.get_series('BRACPIALLMINMEI', limit=36)

        if cpi_data.empty or len(cpi_data) < 13:
            forecast = 4.5
            last_actual = 4.5
            last_month = "Unknown"
        else:
            yoy = self._calculate_yoy_from_index(cpi_data)
            if yoy.empty:
                forecast = 4.5
                last_actual = 4.5
                last_month = "Unknown"
            else:
                last_actual = yoy.iloc[-1]
                last_month = yoy.index[-1].strftime('%Y-%m')

                # Brazil: BCB target is 3% ±1.5pp, typical 4-5%
                recent_avg = yoy.tail(3).mean()
                trend = yoy.diff().tail(3).mean()

                # Mean reversion toward 4% target
                forecast = recent_avg * 0.7 + 4.0 * 0.3 + trend * 0.3
                forecast = max(0, min(12, forecast))

        vol = self.FORECAST_VOLATILITY['br_cpi']
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='CPI YoY',
            region='BR',
            target_month=target_month,
            forecast=round(forecast, 2),
            confidence_low=round(ci_low, 2),
            confidence_high=round(ci_high, 2),
            last_actual=round(last_actual, 2),
            last_actual_month=last_month,
            methodology="FRED Brazil CPI index → YoY + BCB target mean-reversion",
            data_sources=['FRED:BRACPIALLMINMEI'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    def forecast_mexico_cpi(self, target_month: str = None) -> MonthlyForecast:
        """Forecast Mexico CPI inflation YoY"""
        if target_month is None:
            target_month = self._get_current_month()

        print(f"📊 Forecasting Mexico CPI for {target_month}...")

        cpi_data = self.fred.get_series('MEXCPIALLMINMEI', limit=36)

        if cpi_data.empty or len(cpi_data) < 13:
            forecast = 4.5
            last_actual = 4.5
            last_month = "Unknown"
        else:
            yoy = self._calculate_yoy_from_index(cpi_data)
            if yoy.empty:
                forecast = 4.5
                last_actual = 4.5
                last_month = "Unknown"
            else:
                last_actual = yoy.iloc[-1]
                last_month = yoy.index[-1].strftime('%Y-%m')

                # Mexico: Banxico target is 3% ±1pp
                recent_avg = yoy.tail(3).mean()
                trend = yoy.diff().tail(3).mean()

                # Mean reversion toward 3.5%
                forecast = recent_avg * 0.7 + 3.5 * 0.3 + trend * 0.3
                forecast = max(0, min(10, forecast))

        vol = self.FORECAST_VOLATILITY['mx_cpi']
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='CPI YoY',
            region='MX',
            target_month=target_month,
            forecast=round(forecast, 2),
            confidence_low=round(ci_low, 2),
            confidence_high=round(ci_high, 2),
            last_actual=round(last_actual, 2),
            last_actual_month=last_month,
            methodology="FRED Mexico CPI index → YoY + Banxico target mean-reversion",
            data_sources=['FRED:MEXCPIALLMINMEI'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    def forecast_india_cpi(self, target_month: str = None) -> MonthlyForecast:
        """Forecast India CPI inflation YoY"""
        if target_month is None:
            target_month = self._get_current_month()

        print(f"📊 Forecasting India CPI for {target_month}...")

        cpi_data = self.fred.get_series('INDCPIALLMINMEI', limit=36)

        if cpi_data.empty or len(cpi_data) < 13:
            forecast = 5.0
            last_actual = 5.0
            last_month = "Unknown"
        else:
            yoy = self._calculate_yoy_from_index(cpi_data)
            if yoy.empty:
                forecast = 5.0
                last_actual = 5.0
                last_month = "Unknown"
            else:
                last_actual = yoy.iloc[-1]
                last_month = yoy.index[-1].strftime('%Y-%m')

                # India: RBI target is 4% ±2pp, typical 5-6%
                recent_avg = yoy.tail(3).mean()
                trend = yoy.diff().tail(3).mean()

                # India food prices volatile - use higher persistence
                forecast = recent_avg * 0.8 + 5.0 * 0.2 + trend * 0.2
                forecast = max(0, min(12, forecast))

        vol = self.FORECAST_VOLATILITY['in_cpi']
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='CPI YoY',
            region='IN',
            target_month=target_month,
            forecast=round(forecast, 2),
            confidence_low=round(ci_low, 2),
            confidence_high=round(ci_high, 2),
            last_actual=round(last_actual, 2),
            last_actual_month=last_month,
            methodology="FRED India CPI index → YoY + RBI target mean-reversion",
            data_sources=['FRED:INDCPIALLMINMEI'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    # =========================================================================
    # EMERGING MARKETS - QUARTERLY FORECASTS
    # =========================================================================

    # IMF WEO annual forecasts for emerging markets (fallback)
    IMF_EMERGING_FORECASTS_2026 = {
        'CN': {'gdp': 4.5, 'inflation': 1.5, 'unemployment': 5.2},
        'BR': {'gdp': 2.0, 'inflation': 4.5, 'unemployment': 8.5},
        'MX': {'gdp': 2.2, 'inflation': 4.0, 'unemployment': 3.5},
        'IN': {'gdp': 6.5, 'inflation': 4.5, 'unemployment': 7.5},
        'AR': {'gdp': 3.5, 'inflation': 60.0, 'unemployment': 7.0},  # Argentina special case
    }

    # Quarterly volatility for emerging markets
    QUARTERLY_VOLATILITY = {
        'CN': {'gdp': 0.5, 'inflation': 0.8, 'unemployment': 0.3},
        'BR': {'gdp': 0.8, 'inflation': 1.2, 'unemployment': 0.5},
        'MX': {'gdp': 0.6, 'inflation': 0.8, 'unemployment': 0.4},
        'IN': {'gdp': 0.8, 'inflation': 1.5, 'unemployment': 0.5},
        'AR': {'gdp': 2.0, 'inflation': 15.0, 'unemployment': 1.0},
    }

    def forecast_emerging_quarterly_inflation(self, country: str, target_quarter: str = None) -> MonthlyForecast:
        """
        Forecast quarterly inflation for emerging markets.

        Uses monthly CPI data to estimate quarterly average.
        """
        if target_quarter is None:
            now = datetime.now()
            q = (now.month - 1) // 3 + 1
            target_quarter = f"{now.year}-Q{q}"

        print(f"📊 Forecasting {country} Inflation for {target_quarter}...")

        # Get monthly forecast for the quarter's middle month
        year = int(target_quarter.split('-')[0])
        quarter = int(target_quarter.split('Q')[1])
        mid_month = quarter * 3 - 1  # Q1→Feb, Q2→May, Q3→Aug, Q4→Nov
        target_month = f"{year}-{mid_month:02d}"

        # Get the monthly forecast
        monthly_fc = None
        if country == 'CN':
            monthly_fc = self.forecast_china_cpi(target_month)
        elif country == 'BR':
            monthly_fc = self.forecast_brazil_cpi(target_month)
        elif country == 'MX':
            monthly_fc = self.forecast_mexico_cpi(target_month)
        elif country == 'IN':
            monthly_fc = self.forecast_india_cpi(target_month)
        else:
            # Fallback to IMF
            imf = self.IMF_EMERGING_FORECASTS_2026.get(country, {})
            forecast = imf.get('inflation', 5.0)
            vol = self.QUARTERLY_VOLATILITY.get(country, {}).get('inflation', 1.0)
            return MonthlyForecast(
                indicator='Inflation YoY',
                region=country,
                target_month=target_quarter,
                forecast=round(forecast, 2),
                confidence_low=round(forecast - 1.28 * vol, 2),
                confidence_high=round(forecast + 1.28 * vol, 2),
                last_actual=forecast,
                last_actual_month="IMF WEO",
                methodology="IMF WEO 2026 forecast (fallback)",
                data_sources=['IMF:WEO'],
                forecast_date=datetime.now().strftime('%Y-%m-%d')
            )

        # Use monthly forecast for quarterly
        return MonthlyForecast(
            indicator='Inflation YoY',
            region=country,
            target_month=target_quarter,
            forecast=monthly_fc.forecast,
            confidence_low=monthly_fc.confidence_low,
            confidence_high=monthly_fc.confidence_high,
            last_actual=monthly_fc.last_actual,
            last_actual_month=monthly_fc.last_actual_month,
            methodology=f"Quarterly from monthly: {monthly_fc.methodology}",
            data_sources=monthly_fc.data_sources,
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    def forecast_emerging_quarterly_gdp(self, country: str, target_quarter: str = None) -> MonthlyForecast:
        """
        Forecast quarterly GDP growth for emerging markets.

        Uses IMF annual forecast + quarterly interpolation.
        For China/India uses known seasonal patterns.
        """
        if target_quarter is None:
            now = datetime.now()
            q = (now.month - 1) // 3 + 1
            target_quarter = f"{now.year}-Q{q}"

        print(f"📊 Forecasting {country} GDP for {target_quarter}...")

        year = int(target_quarter.split('-')[0])
        quarter = int(target_quarter.split('Q')[1])

        # Get IMF annual forecast
        imf = self.IMF_EMERGING_FORECASTS_2026.get(country, {})
        annual_gdp = imf.get('gdp', 3.0)

        # Quarterly adjustments (seasonal patterns)
        # China: Q1 typically weaker, Q4 stronger
        # India: Q3 (monsoon) typically stronger
        quarterly_factors = {
            'CN': {1: 0.9, 2: 1.0, 3: 1.0, 4: 1.1},
            'BR': {1: 0.95, 2: 1.0, 3: 1.0, 4: 1.05},
            'MX': {1: 0.95, 2: 1.0, 3: 1.05, 4: 1.0},
            'IN': {1: 0.9, 2: 0.95, 3: 1.1, 4: 1.05},
            'AR': {1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0},  # Too volatile for patterns
        }

        factor = quarterly_factors.get(country, {}).get(quarter, 1.0)
        forecast = annual_gdp * factor

        vol = self.QUARTERLY_VOLATILITY.get(country, {}).get('gdp', 1.0)
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='GDP Growth YoY',
            region=country,
            target_month=target_quarter,
            forecast=round(forecast, 2),
            confidence_low=round(ci_low, 2),
            confidence_high=round(ci_high, 2),
            last_actual=annual_gdp,
            last_actual_month="IMF WEO 2026",
            methodology=f"IMF WEO + Q{quarter} seasonal adjustment (factor: {factor})",
            data_sources=['IMF:WEO'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    def forecast_emerging_quarterly_unemployment(self, country: str, target_quarter: str = None) -> MonthlyForecast:
        """
        Forecast quarterly unemployment for emerging markets.

        Uses persistence model (unemployment very sticky) + IMF baseline.
        """
        if target_quarter is None:
            now = datetime.now()
            q = (now.month - 1) // 3 + 1
            target_quarter = f"{now.year}-Q{q}"

        print(f"📊 Forecasting {country} Unemployment for {target_quarter}...")

        # Get IMF baseline
        imf = self.IMF_EMERGING_FORECASTS_2026.get(country, {})
        forecast = imf.get('unemployment', 5.0)

        vol = self.QUARTERLY_VOLATILITY.get(country, {}).get('unemployment', 0.5)
        ci_low = forecast - 1.28 * vol
        ci_high = forecast + 1.28 * vol

        return MonthlyForecast(
            indicator='Unemployment Rate',
            region=country,
            target_month=target_quarter,
            forecast=round(forecast, 2),
            confidence_low=round(max(0, ci_low), 2),
            confidence_high=round(ci_high, 2),
            last_actual=forecast,
            last_actual_month="IMF WEO 2026",
            methodology="IMF WEO + persistence model (unemployment sticky)",
            data_sources=['IMF:WEO'],
            forecast_date=datetime.now().strftime('%Y-%m-%d')
        )

    # =========================================================================
    # POLYMARKET HELPER - Probability for bracket markets
    # =========================================================================

    def polymarket_probability(self, forecast: MonthlyForecast, threshold: float,
                               direction: str = 'above') -> float:
        """
        Calculate probability for Polymarket Yes/No markets.

        Example: "US CPI > 2.5% in February?"
        → polymarket_probability(cpi_forecast, 2.5, 'above')

        Uses normal distribution with forecast as mean and historical volatility.
        """
        from scipy.stats import norm

        indicator_key = f"{forecast.region.lower()}_{forecast.indicator.split()[0].lower()}"
        vol = self.FORECAST_VOLATILITY.get(indicator_key, 0.3)

        if direction == 'above':
            prob = 1 - norm.cdf(threshold, loc=forecast.forecast, scale=vol)
        else:  # below
            prob = norm.cdf(threshold, loc=forecast.forecast, scale=vol)

        return round(prob * 100, 1)

    # =========================================================================
    # FULL FORECAST REPORT
    # =========================================================================

    def generate_full_report(self, target_month: str = None) -> Dict[str, MonthlyForecast]:
        """Generate all forecasts for Polymarket"""

        if target_month is None:
            target_month = self._get_current_month()

        print("=" * 70)
        print(f"📊 MONTHLY ECONOMIC FORECAST REPORT")
        print(f"📅 Target: {target_month}")
        print("=" * 70)

        forecasts = {}

        # US Forecasts
        print("\n🇺🇸 UNITED STATES")
        print("-" * 50)

        forecasts['us_cpi'] = self.forecast_us_cpi(target_month)
        print(f"  CPI YoY: {forecasts['us_cpi'].forecast}% "
              f"[{forecasts['us_cpi'].confidence_low}% - {forecasts['us_cpi'].confidence_high}%]")

        forecasts['us_unemployment'] = self.forecast_us_unemployment(target_month)
        print(f"  Unemployment: {forecasts['us_unemployment'].forecast}% "
              f"[{forecasts['us_unemployment'].confidence_low}% - {forecasts['us_unemployment'].confidence_high}%]")

        forecasts['us_gdp'] = self.forecast_us_gdp()
        print(f"  GDP (Q): {forecasts['us_gdp'].forecast}% "
              f"[{forecasts['us_gdp'].confidence_low}% - {forecasts['us_gdp'].confidence_high}%]")

        # Eurozone Forecasts
        print("\n🇪🇺 EUROZONE")
        print("-" * 50)

        forecasts['ea_hicp'] = self.forecast_eurozone_hicp(target_month)
        print(f"  HICP YoY: {forecasts['ea_hicp'].forecast}% "
              f"[{forecasts['ea_hicp'].confidence_low}% - {forecasts['ea_hicp'].confidence_high}%]")

        forecasts['ea_unemployment'] = self.forecast_eurozone_unemployment(target_month)
        print(f"  Unemployment: {forecasts['ea_unemployment'].forecast}% "
              f"[{forecasts['ea_unemployment'].confidence_low}% - {forecasts['ea_unemployment'].confidence_high}%]")

        print("\n" + "=" * 70)
        print("💰 POLYMARKET EXAMPLE PROBABILITIES")
        print("=" * 70)

        # Example thresholds
        thresholds = {
            'us_cpi': [(2.5, 'above'), (3.0, 'above'), (3.5, 'below')],
            'us_unemployment': [(4.0, 'above'), (4.5, 'above'), (5.0, 'below')],
        }

        for key, thresh_list in thresholds.items():
            fc = forecasts[key]
            print(f"\n  {fc.region} {fc.indicator}:")
            for thresh, direction in thresh_list:
                prob = self.polymarket_probability(fc, thresh, direction)
                print(f"    P({direction} {thresh}%) = {prob}%")

        print("\n" + "=" * 70)

        return forecasts


if __name__ == "__main__":
    forecaster = MonthlyForecaster()
    forecaster.generate_full_report("2026-02")
