# StatData MCP

Unified MCP connector for **Eurostat** (EU statistics + news/press flash releases), **KSH** (Hungarian Central Statistical Office, STADAT + HVD + gyorstájékoztatók RSS), **DBnomics** (700M+ series from 70+ global providers: IMF, ECB, OECD, World Bank, etc.), **MNB** (Hungarian National Bank), **ECB Data Portal** (direct SDMX — HICP incl. services, EUR FX, policy rates, govt bond yields), **FRED** (US Federal Reserve), **BIS** (policy rates), and **Yahoo Finance** (stocks, forex, commodities, indices).

## Tools

| Tool | Description |
|------|-------------|
| `search_datasets` | Search Eurostat, KSH, DBnomics, ECB, and flash releases by keyword |
| `get_eurostat_data` | Fetch Eurostat data with filters (country, time, dimensions) — also COMEXT sub-mode |
| `get_ksh_stadat` | KSH STADAT tables — Hungarian time series (prices, wages, GDP...) |
| `get_ksh_hvd` | KSH High-Value Datasets — list/search/download (CSV/SDMX) |
| `dbnomics_search` | Search datasets across all DBnomics providers (also `mode="providers"`) |
| `dbnomics_series` | Fetch time series data with dimension filters |
| `get_ecb_data` | **NEW:** ECB Data Portal direct SDMX — HICP incl. services sub-aggregate, EUR FX reference, ECB policy rates (DFR/MRR/MLFR), Maastricht 10Y yields, MFI balance sheets |
| `get_flash_releases` | **NEW:** KSH gyorstájékoztatók (RSS) + Eurostat news/press releases (Atom) — freshest HU/EA macro numbers, 1–3 days ahead of official APIs |
| `get_policy_rates` | Central bank policy rates (BIS) — ECB euro-area rate always overlaid with direct ECB DFR (never stale) |
| `get_fred_data` | FRED US economic data (800K+ series) |
| `mnb_rates` | MNB exchange rates — current + historical (1949–) |
| `forecast` | Macro forecasts (GDP, inflation, unemployment) + OECD CLI |
| `get_economic_calendar` | Upcoming data releases (FRED, ECB, Eurostat) |
| `yfinance` | Yahoo Finance quotes + OHLCV history (stocks, FX, commodities, crypto, indices) |
| `calculate` | Economic calculator (inflation, CAGR, real value, conversion) |
| `recipe_book` | Self-learning recipe book (38+ seed recipes, AI-extensible) |

## Quick Start

### Local
```bash
pip install -r requirements.txt
python server.py
# → http://localhost:8000/mcp
```

### Railway
```bash
# Push to GitHub, connect to Railway — auto-detects Dockerfile, sets PORT
```

### Connect to Claude Desktop
```json
{
  "mcpServers": {
    "statdata": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://YOUR-URL/mcp"]
    }
  }
}
```

### Connect to Claude Web / ChatGPT
Add as MCP integration: `https://YOUR-URL/mcp`

## Usage Examples

```
search_datasets(query="GDP Hungary", source="all")
get_eurostat_data(dataset_code="nama_10_gdp", geo="HU", time="2023")
get_ksh_stadat(table_code="ara0001")
dbnomics_series(provider_code="IMF", dataset_code="WEO:latest", series_code="A.HU.NGDP_RPCH")
yfinance(action="quote", symbol="EURHUF=X")

# Direct ECB — covers gaps where Eurostat/DBnomics lag
get_ecb_data(dataset="ICP", key="M.HU.N.SERV00.4.ANR")    # HU monthly service inflation YoY%
get_ecb_data(dataset="ICP", key="M.HU.N.XEF000.4.ANR")    # HU core HICP (excl. energy & food)
get_ecb_data(dataset="FM",  key="D.U2.EUR.4F.KR.DFR.LEV") # ECB Deposit Facility Rate (daily)
get_ecb_data(dataset="EXR", key="D.HUF.EUR.SP00.A")       # EUR/HUF daily reference rate
get_ecb_data(dataset="IRS", key="M.HU.L.L40.CI.0000.HUF.N.Z") # HU 10Y Maastricht yield

# Flash releases — for 2026 data that isn't yet in the APIs
get_flash_releases(query="fogyasztói árak", source="ksh")   # HU CPI flash
get_flash_releases(query="unemployment",    source="eurostat") # EA unemployment flash
get_flash_releases(query="", source="all", limit=20)         # latest 20 from both feeds
```

## Data Sources

| Source | API | Auth |
|--------|-----|------|
| Eurostat | JSON-stat v2 + Atom news feed | None |
| KSH STADAT | CSV (semicolon, Win-1250) | None |
| KSH HVD | JSON + SDMX | None |
| KSH gyorstájékoztatók | RSS (ISO-8859-2) | None |
| DBnomics | REST JSON | None |
| ECB Data Portal | SDMX 2.1 (jsondata) | None |
| BIS | via DBnomics | None |
| FRED | REST JSON | API key |
| MNB | SOAP | None |
| Yahoo Finance | yfinance Python lib | None |

## License

MIT — Data subject to respective provider copyright policies.
