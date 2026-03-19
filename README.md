# Statisztikai Adatok MCP

Unified MCP connector for **Eurostat** (EU statistics), **KSH** (Hungarian Central Statistical Office), **DBnomics** (700M+ series from 70+ global providers: IMF, ECB, OECD, World Bank, etc.), and **Yahoo Finance** (stocks, forex, commodities, indices).

## Tools

| Tool | Description |
|------|-------------|
| `search_datasets` | Search Eurostat, KSH, and/or DBnomics datasets by keyword |
| `get_eurostat_data` | Fetch Eurostat data with filters (country, time, dimensions) |
| `get_ksh_stadat` | KSH STADAT tables — Hungarian time series (prices, wages, GDP...) |
| `get_ksh_datasets` | List/search KSH High-Value Datasets |
| `get_ksh_data` | Download KSH HVD dataset as parsed CSV/SDMX |
| `dbnomics_providers` | List all 70+ DBnomics data providers |
| `dbnomics_search` | Search datasets across all DBnomics providers |
| `dbnomics_series` | Fetch time series data with dimension filters |
| `yfinance_quote` | Current quote & stats (stocks, FX, commodities, crypto, indices) |
| `yfinance_history` | Historical OHLCV price data (daily/weekly/monthly) |

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
    "statisztika": {
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
yfinance_quote(symbol="EURHUF=X")
yfinance_history(symbol="^BUX", period="1y")
```

## Data Sources

| Source | API | Auth |
|--------|-----|------|
| Eurostat | JSON-stat v2 | None |
| KSH STADAT | CSV (semicolon, Win-1250) | None |
| KSH HVD | JSON + SDMX | None |
| DBnomics | REST JSON | None |
| Yahoo Finance | yfinance Python lib | None |

## License

MIT — Data subject to respective provider copyright policies.
