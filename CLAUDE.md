# CLAUDE.md — Statisztikai Adatok MCP Server

## Project Overview

A unified MCP (Model Context Protocol) server providing AI assistants with access to **700M+ data series** from European and global statistical sources. Focused on Hungarian macroeconomic data with broad international coverage.

**Language**: Python 3.12 | **Framework**: FastMCP | **Transport**: Streamable HTTP on port 8000

## Repository Structure

```
StatadatokMCP/
├── server.py           # Complete MCP server implementation (~2150 lines)
├── requirements.txt    # Python dependencies
├── README.md           # User-facing documentation
├── CLAUDE.md           # This file — AI assistant guide
├── Dockerfile          # Python 3.12-slim container
├── railway.json        # Railway.app deployment config
└── .gitignore          # Python exclusions
```

**Note**: This is a single-file server. All logic lives in `server.py` — no modules or subdirectories.

## Dependencies

```
mcp>=1.20.0       # Model Context Protocol SDK
httpx>=0.27.0     # Async HTTP client
uvicorn>=0.30.0   # ASGI server
yfinance>=0.2.0   # Yahoo Finance data
mnb>=1.0.0        # Magyar Nemzeti Bank exchange rates
```

## Running Locally

```bash
pip install -r requirements.txt
python server.py
# Server: http://localhost:8000/mcp
# Landing page: http://localhost:8000/
```

## Data Sources & Tools (12 MCP tools)

### Search & Discovery
| Tool | Description |
|------|-------------|
| `search_datasets(query, source, limit)` | Unified search across Eurostat, KSH, DBnomics |
| `dbnomics_providers(query)` | List/filter 70+ DBnomics data providers |

### Eurostat (EU statistics)
| Tool | Description |
|------|-------------|
| `get_eurostat_data(dataset_code, geo, time, ...)` | Fetch EU data via JSON-stat v2 API |

### KSH (Hungarian Central Statistics Office)
| Tool | Description |
|------|-------------|
| `get_ksh_stadat(table_code, max_rows)` | Hungarian time series from STADAT (CSV) |
| `get_ksh_datasets(query, lang)` | Search KSH High-Value Datasets |
| `get_ksh_data(dataset_id, max_rows)` | Download KSH HVD content (CSV/SDMX-XML) |

### DBnomics (700M+ series, 70+ providers)
| Tool | Description |
|------|-------------|
| `dbnomics_search(query, provider, limit)` | Search across all DBnomics providers |
| `dbnomics_series(provider_code, dataset_code, ...)` | Fetch time series with dimension filters |

### Yahoo Finance
| Tool | Description |
|------|-------------|
| `yfinance_quote(symbol)` | Current price & stats for stocks/forex/crypto/commodities |
| `yfinance_history(symbol, period, interval, ...)` | Historical OHLCV price data |

### MNB (Hungarian National Bank)
| Tool | Description |
|------|-------------|
| `mnb_current_rates(currencies)` | Official HUF exchange rates (32+ currencies) |
| `mnb_historical_rates(start_date, end_date, currencies)` | Historical rates since 1949 |

### Calculator
| Tool | Description |
|------|-------------|
| `calculate(expression)` | Math + economic functions: `cum_inflation()`, `real_value()`, `cagr()`, `convert()`, `pct_change()` |

## Architecture & Key Implementation Details

### Server Setup
- Uses `FastMCP` from `mcp.server.fastmcp` with Streamable HTTP transport
- Global shared `httpx.AsyncClient` with 60s timeout and connection pooling
- Port configurable via `PORT` env var (default 8000)

### Caching Strategy
| Cache | TTL | Storage |
|-------|-----|---------|
| Eurostat Table of Contents | 24h | In-memory |
| KSH datasets | 24h | In-memory |
| DBnomics providers | 24h | In-memory |
| KSH STADAT table index | 7 days | SQLite at `/tmp/ksh_stadat_index.db` |

### KSH STADAT Discovery System
- Auto-discovers tables by scanning all 27 KSH category prefixes (ara, gdp, mun, ipa, epi, bel, kkr, ene, nep, tur, etc.)
- Runs as background async task on first `search_datasets` call
- Batches of 50 per category, 3 categories concurrent, stops after 3 consecutive empty batches
- Falls back to 100+ hardcoded static catalog entries
- Results stored in SQLite for fast keyword search

### Data Parsing Specifics
- **Eurostat**: JSON-stat v2 multi-dimensional → flat rows (max 500 rows)
- **KSH CSV**: Windows-1250 encoding, semicolon delimiter, Hungarian number format (space=thousands, comma=decimal), handles matrix/territorial format
- **SDMX-XML**: Regex-based parsing (no XML library dependency)
- **DBnomics**: Truncates to last 100 observations
- **Yahoo Finance**: Rounds to 4 decimals, truncates to 500 data points

### Landing Page
- `GET /` serves an interactive HTML page with integration instructions
- Dark theme with animated gradient orbs
- Copy-to-clipboard config generator for Claude Desktop, Claude Web, ChatGPT

## Development Conventions

### Code Style
- Single-file architecture — all code stays in `server.py`
- Async functions for all HTTP operations
- Hungarian comments and variable names appear in KSH-related code
- Error messages include hints and suggestions for the user

### Adding a New Data Source
1. Add dependency to `requirements.txt`
2. Add tool function(s) in `server.py` using `@mcp.tool()` decorator
3. Include the tool in the landing page HTML table
4. Update `README.md` with usage examples

### Adding a New KSH Table
- Add to the `KNOWN_KSH_TABLES` dictionary with format: `"code": "Description"`
- Tables follow category prefix convention: `ara` (prices), `gdp` (GDP), `mun` (labor), etc.

### Error Handling Patterns
- HTTP errors → return status code + helpful hints
- Invalid parameters → show suggestions and valid values
- Eurostat async datasets → return warning message
- Unknown tickers → detect and report clearly

## Deployment

### Docker
```bash
docker build -t statisztika .
docker run -p 8000:8000 statisztika
```

### Railway
- Auto-detects `Dockerfile`
- Config in `railway.json`: restart on failure, max 10 retries
- Sets `PORT` env var automatically

## Known Limitations
- Eurostat: max 500 rows per response; async datasets return a warning instead of data
- DBnomics: observations truncated to last 100
- Yahoo Finance: history truncated to last 500 data points
- KSH STADAT: SQLite index lives in `/tmp` (ephemeral in containers, rebuilds automatically)
- No authentication required for any data source
- No test suite exists — testing is manual via MCP tool calls
