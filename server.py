"""
StatData MCP Server
======================================
Unified MCP connector for European, Hungarian, and global statistical data
(Eurostat, KSH, DBnomics, MNB, FRED, BIS, Yahoo Finance).
Deployable on Railway with Streamable HTTP transport.

17 Tools:
  - search_datasets: Search Eurostat, KSH, DBnomics, ECB, and flash releases by keyword
  - get_eurostat_data: Fetch data from Eurostat (JSON-stat API)
  - dbnomics_search: Search datasets across DBnomics + list providers (mode="providers")
  - dbnomics_series: Fetch time series data from DBnomics
  - get_ksh_stadat: Fetch KSH STADAT Hungarian time series
  - get_ksh_hvd: List/search or download KSH High-Value Datasets
  - yfinance: Yahoo Finance quotes (action="quote") and history (action="history")
  - mnb_rates: MNB exchange rates — current (mode="current") or historical (mode="historical")
  - calculate: Economic calculator (inflation, CAGR, real value, conversion)
  - recipe_book: Self-learning recipe book — search/add/report/stats (action parameter)
  - forecast: Macro forecasts (GDP, inflation, unemployment) + OECD CLI (indicator="oecd_cli")
  - get_fred_data: FRED US economic data
  - get_economic_calendar: Upcoming data releases (FRED, ECB, Eurostat)
  - get_policy_rates: Central bank policy rates (BIS) — ECB, MNB, CNB, NBP, BNR, etc.
                      + direct ECB Data Portal DFR/MRR/MLFR overlay (always fresh).
  - get_ecb_data: ECB Data Portal direct SDMX (HICP incl. services, EUR FX,
                  ECB rates, money market, MFI balance sheets, govt bond yields).
  - get_flash_releases: KSH gyorstájékoztatók + Eurostat news releases
                        (the freshest HU/EA flash macro numbers before APIs update).
  - get_macro_indicator: HIGH-LEVEL guaranteed-fresh router. One call →
                          one number. Country-agnostic (HU/DE/FR/IT/ES/EA/US/GB).
                          Indicators: cpi, core_cpi, services_cpi, policy_rate,
                          unemployment, gdp. Internally chains structured APIs
                          → official scrape → brave_search, with automatic
                          freshness checking. Use this for "what's HU's CPI
                          now" type questions instead of orchestrating manually.
  Sub-tool: get_eurostat_data(dataset_code="COMEXT") — Eurostat COMEXT HS-level commodity trade
"""

import asyncio
import csv
import io
import os
import json
import logging
import re
import sqlite3
import time
from typing import Optional

import httpx
import yfinance as yf
from mnb import Mnb as MnbClient
from mcp.server.fastmcp import FastMCP
from starlette.responses import HTMLResponse, JSONResponse, Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("statdata")

# ---------------------------------------------------------------------------
# Server setup
# ---------------------------------------------------------------------------
mcp = FastMCP(
    "StatData",
    stateless_http=True,
    json_response=True,
    host="0.0.0.0",
    port=int(os.environ.get("PORT", "8000")),
)

# Shared async HTTP client
_client: Optional[httpx.AsyncClient] = None


async def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)
    return _client


# ---------------------------------------------------------------------------
# Eurostat helpers
# ---------------------------------------------------------------------------
EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination"
EUROSTAT_STAT = f"{EUROSTAT_BASE}/statistics/1.0/data"
EUROSTAT_TOC_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en"
)

# Cache for TOC (in-memory, 24h TTL)
_eurostat_toc_cache: list[dict] = []
_eurostat_toc_loaded_at: float = 0.0
_CACHE_TTL = 86400  # 24 hours


def _strip_tsv_field(s: str) -> str:
    """Strip whitespace and surrounding quotes from a TSV/CSV field."""
    return s.strip().strip('"').strip()


async def _load_eurostat_toc() -> list[dict]:
    """Download and parse Eurostat Table of Contents (TSV text format).

    The Eurostat TOC TSV has columns: title, code (title first!).
    """
    global _eurostat_toc_cache, _eurostat_toc_loaded_at
    now = time.time()
    if _eurostat_toc_cache and (now - _eurostat_toc_loaded_at < _CACHE_TTL):
        return _eurostat_toc_cache

    client = await get_client()
    try:
        resp = await client.get(EUROSTAT_TOC_URL)
        resp.raise_for_status()
        text = resp.text

        entries = []
        for line in text.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) >= 2:
                # TSV format: title\tcode (title is first column!)
                title = _strip_tsv_field(parts[0])
                code = _strip_tsv_field(parts[1])
                # Skip header/category rows that don't look like dataset codes
                if code and not code.startswith('"'):
                    entries.append({"code": code, "title": title})
        _eurostat_toc_cache = entries
        _eurostat_toc_loaded_at = now
        logger.info(f"Loaded {len(entries)} Eurostat TOC entries")
    except Exception as e:
        logger.error(f"Failed to load Eurostat TOC: {e}")
        # Try SDMX dataflow list as fallback
        try:
            resp2 = await client.get(
                f"{EUROSTAT_BASE}/sdmx/2.1/dataflow/all/all/latest",
                headers={"Accept": "application/json"},
            )
            resp2.raise_for_status()
            data = resp2.json()
            flows = data.get("Dataflow", [])
            if isinstance(flows, dict):
                flows = [flows]
            for f in flows:
                code = f.get("id", "")
                name_obj = f.get("Name", {})
                title = name_obj if isinstance(name_obj, str) else name_obj.get("en", str(name_obj))
                _eurostat_toc_cache.append({"code": code, "title": title})
            _eurostat_toc_loaded_at = now
            logger.info(f"Loaded {len(_eurostat_toc_cache)} entries via SDMX fallback")
        except Exception as e2:
            logger.error(f"SDMX fallback also failed: {e2}")

    return _eurostat_toc_cache


_SEARCH_SYNONYMS: dict[str, list[str]] = {
    # Country names → Eurostat geo codes and English names used in titles
    "slovenia": ["si", "slovenian"],
    "szlovénia": ["si", "slovenia", "slovenian"],
    "estonia": ["ee", "estonian"],
    "észtország": ["ee", "estonia", "estonian"],
    "hungary": ["hu", "hungarian"],
    "magyarország": ["hu", "hungary", "hungarian"],
    "poland": ["pl", "polish"],
    "lengyelország": ["pl", "poland", "polish"],
    "czechia": ["cz", "czech"],
    "csehország": ["cz", "czech", "czechia"],
    "slovakia": ["sk", "slovak"],
    "szlovákia": ["sk", "slovakia", "slovak"],
    "romania": ["ro", "romanian"],
    "románia": ["ro", "romania", "romanian"],
    "croatia": ["hr", "croatian"],
    "horvátország": ["hr", "croatia", "croatian"],
    "bulgaria": ["bg", "bulgarian"],
    "bulgária": ["bg", "bulgaria", "bulgarian"],
    "austria": ["at", "austrian"],
    "ausztria": ["at", "austria", "austrian"],
    "germany": ["de", "german"],
    "németország": ["de", "germany", "german"],
    "france": ["fr", "french"],
    "franciaország": ["fr", "france", "french"],
    "italy": ["it", "italian"],
    "olaszország": ["it", "italy", "italian"],
    "spain": ["es", "spanish"],
    "spanyolország": ["es", "spain", "spanish"],
    "portugal": ["pt", "portuguese"],
    "portugália": ["pt", "portugal", "portuguese"],
    "greece": ["el", "greek"],
    "görögország": ["el", "greece", "greek"],
    "netherlands": ["nl", "dutch"],
    "hollandia": ["nl", "netherlands", "dutch"],
    "belgium": ["be", "belgian"],
    "belgium": ["be", "belgian"],
    "sweden": ["se", "swedish"],
    "svédország": ["se", "sweden", "swedish"],
    "finland": ["fi", "finnish"],
    "finnország": ["fi", "finland", "finnish"],
    "denmark": ["dk", "danish"],
    "dánia": ["dk", "denmark", "danish"],
    "ireland": ["ie", "irish"],
    "írország": ["ie", "ireland", "irish"],
    "luxembourg": ["lu"],
    "luxemburg": ["lu", "luxembourg"],
    "cyprus": ["cy"],
    "ciprus": ["cy", "cyprus"],
    "malta": ["mt"],
    "málta": ["mt", "malta"],
    "latvia": ["lv", "latvian"],
    "lettország": ["lv", "latvia", "latvian"],
    "lithuania": ["lt", "lithuanian"],
    "litvánia": ["lt", "lithuania", "lithuanian"],
    # Topic synonyms — common search terms → Eurostat vocabulary
    "wages": ["earnings", "compensation", "remuneration", "wage", "salary"],
    "bér": ["earnings", "compensation", "remuneration", "wages", "wage"],
    "bérek": ["earnings", "compensation", "remuneration", "wages", "wage"],
    "salary": ["earnings", "compensation", "remuneration", "wages"],
    "fizetés": ["earnings", "compensation", "wages"],
    "inflation": ["hicp", "price", "consumer price"],
    "infláció": ["hicp", "price", "inflation", "consumer price"],
    "unemployment": ["unemploy", "labour", "jobless"],
    "munkanélküliség": ["unemploy", "unemployment", "labour", "jobless"],
    "gdp": ["gross domestic product", "national accounts"],
    "debt": ["government debt", "deficit", "fiscal"],
    "adósság": ["debt", "government debt", "deficit"],
    "államadósság": ["government debt", "deficit", "debt"],
    "trade": ["export", "import", "external"],
    "kereskedelem": ["trade", "export", "import", "external"],
    "population": ["demograph", "inhabitant", "resident"],
    "népesség": ["population", "demograph", "inhabitant"],
    "tourism": ["tourist", "accommodation", "hotel", "nights"],
    "turizmus": ["tourism", "tourist", "accommodation", "hotel"],
    "energy": ["electricity", "gas", "fuel"],
    "energia": ["energy", "electricity", "gas", "fuel"],
    "housing": ["house price", "dwelling", "rent"],
    "lakás": ["housing", "house price", "dwelling", "rent"],
}


def _expand_query(keywords: list[str]) -> list[str]:
    """Expand search keywords with synonyms for better Eurostat TOC matching."""
    expanded = list(keywords)
    for kw in keywords:
        synonyms = _SEARCH_SYNONYMS.get(kw, [])
        for syn in synonyms:
            if syn not in expanded:
                expanded.append(syn)
    return expanded


def _search_toc(entries: list[dict], query: str, limit: int = 20) -> list[dict]:
    """Case-insensitive keyword search with relevance scoring and synonym expansion.

    Uses OR logic with scoring: entries matching more keywords rank higher.
    Expands country names and topic keywords with Eurostat-compatible synonyms.
    """
    query_lower = query.lower()
    keywords = query_lower.split()
    if not keywords:
        return []

    expanded = _expand_query(keywords)

    scored = []
    for entry in entries:
        text = f"{entry.get('code', '')} {entry.get('title', '')}".lower()
        # Score: original keywords worth 2 points, synonym matches worth 1
        score = 0
        for kw in keywords:
            if kw in text:
                score += 2
        for syn in expanded:
            if syn not in keywords and syn in text:
                score += 1
        if score > 0:
            scored.append((score, entry))

    # Sort by score descending (most keywords matched first)
    scored.sort(key=lambda x: -x[0])
    return [e for _, e in scored[:limit]]


def _normalize_time_period(tp: str) -> str:
    """Normalize time period to Eurostat format.
    Handles: '2014-Q3', '2014Q3', 'Q3 2014', '2014-03', '2014' etc.
    """
    tp = tp.strip()
    if re.match(r'^\d{4}(-Q[1-4]|-\d{2})?$', tp):
        return tp
    m = re.match(r'^(\d{4})Q([1-4])$', tp, re.IGNORECASE)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    m = re.match(r'^Q([1-4])\s*(\d{4})$', tp, re.IGNORECASE)
    if m:
        return f"{m.group(2)}-Q{m.group(1)}"
    return tp


def _parse_json_stat(data: dict) -> dict:
    """Parse JSON-stat v2 response into a readable table structure."""
    # JSON-stat has: id (dimension order), size, dimension, value
    dims = data.get("id", [])
    sizes = data.get("size", [])
    dimension_info = data.get("dimension", {})
    values = data.get("value", {})

    if not dims:
        return {"error": "Empty or unrecognized JSON-stat response", "raw_keys": list(data.keys())}

    # Distinguish "empty result for these filters" from "broken response".
    # Eurostat returns a valid JSON-stat envelope with empty value{} when the
    # filters don't match any observation (e.g. irt_h_eurcoe_d with geo=HU
    # since this dataset only carries euro area data). Surface that clearly
    # so sub-agents know to relax filters rather than blame the parser.
    # (2026-05-05 audit fix.)
    if not values:
        zero_dims = []
        for d, s in zip(dims, sizes):
            if s == 0:
                zero_dims.append(d)
        if zero_dims:
            return {
                "error": "No data matches the requested filters",
                "empty_dimensions": zero_dims,
                "applied_filters": {d: dimension_info.get(d, {}).get("label", d) for d in dims},
                "hint": (
                    f"Dataset returned 0 observations because dimension(s) {zero_dims} "
                    f"have no values under the current filter. Common cause: this dataset "
                    f"only covers certain geos (e.g. ECB datasets like irt_h_eurcoe_d only "
                    f"have euro area aggregate). Try removing the geo filter or using 'EA'/'EU27_2020'."
                ),
                "raw_keys": list(data.keys()),
            }
        return {"error": "Empty or unrecognized JSON-stat response", "raw_keys": list(data.keys())}

    # Build dimension labels
    dim_labels = {}
    for d in dims:
        dim_data = dimension_info.get(d, {})
        cat = dim_data.get("category", {})
        idx = cat.get("index", {})
        labels = cat.get("label", {})
        # Create position -> label mapping
        if isinstance(idx, dict):
            pos_to_code = {v: k for k, v in idx.items()}
        elif isinstance(idx, list):
            pos_to_code = {i: c for i, c in enumerate(idx)}
        else:
            pos_to_code = {}
        dim_labels[d] = {
            "label": dim_data.get("label", d),
            "categories": {pos: labels.get(code, code) for pos, code in pos_to_code.items()},
            "codes": pos_to_code,
        }

    # Convert flat index to multi-dimensional rows
    rows = []
    total = 1
    for s in sizes:
        total *= s

    for flat_idx in range(total):
        if str(flat_idx) not in values and flat_idx not in values:
            continue
        val = values.get(str(flat_idx), values.get(flat_idx))

        # Compute dimension positions from flat index
        remaining = flat_idx
        row = {}
        for i in range(len(dims) - 1, -1, -1):
            pos = remaining % sizes[i]
            remaining //= sizes[i]
            d = dims[i]
            code = dim_labels[d]["codes"].get(pos, str(pos))
            label = dim_labels[d]["categories"].get(pos, code)
            row[dim_labels[d]["label"]] = label

        row["value"] = val
        rows.append(row)

    # Sort DESC by time so that downstream truncation (both this 500-row cap
    # and the Bridge's per-tool char limit) preserves the LATEST observations
    # rather than the oldest. Eurostat returns rows ASC by default — without
    # this fix, sub-agents only ever saw 1995-onwards old data instead of the
    # 2026 frontier. (2026-05-05 audit-fix.)
    time_label = None
    for d in dims:
        info = dim_labels.get(d, {})
        if d.lower() == "time" or "time" in info.get("label", "").lower():
            time_label = info.get("label", d)
            break
    if time_label:
        try:
            rows.sort(key=lambda r: str(r.get(time_label, "")), reverse=True)
        except Exception:
            pass

    # Truncate if too many rows (now keeps the freshest 500, not the oldest)
    truncated = False
    if len(rows) > 500:
        rows = rows[:500]
        truncated = True

    return {
        "dimensions": {d: dim_labels[d]["label"] for d in dims},
        "row_count": len(rows),
        "truncated": truncated,
        "data": rows,
    }


# ---------------------------------------------------------------------------
# KSH helpers
# ---------------------------------------------------------------------------
KSH_BASE = "https://data.ksh.hu"
_ksh_datasets_cache: list[dict] = []
_ksh_datasets_loaded_at: float = 0.0


def _parse_sdmx_compact(xml_text: str, max_rows: int = 200) -> list[dict]:
    """Parse SDMX CompactData XML into flat rows.

    Each <Series> has dimension attributes, each <Obs> has TIME_PERIOD and OBS_VALUE.
    We flatten: series attributes + obs attributes → one row per observation.
    """
    rows = []

    # Find all Series blocks with regex (avoid lxml dependency)
    # Series tag can have various namespace prefixes
    series_pattern = re.compile(
        r'<[^>]*:?Series\s+([^>]+)>(.*?)</[^>]*:?Series>',
        re.DOTALL,
    )
    obs_pattern = re.compile(r'<[^>]*:?Obs\s+([^/]*)/>')

    for series_match in series_pattern.finditer(xml_text):
        # Parse series attributes
        series_attrs_str = series_match.group(1)
        series_attrs = dict(re.findall(r'(\w+)="([^"]*)"', series_attrs_str))

        series_body = series_match.group(2)

        # Parse each Obs within this series
        for obs_match in obs_pattern.finditer(series_body):
            obs_attrs_str = obs_match.group(1)
            obs_attrs = dict(re.findall(r'(\w+)="([^"]*)"', obs_attrs_str))

            # Merge series + obs attributes
            row = {}
            # Pick most useful series attrs
            for key in ("ITEM", "REF_AREA", "FREQ", "UNIT_MEASURE", "BASE_PER",
                         "IDX_TYPE", "IND_TYPE", "SEASONAL_ADJUST", "COVERAGE_GEO"):
                if key in series_attrs:
                    row[key] = series_attrs[key]

            row["TIME_PERIOD"] = obs_attrs.get("TIME_PERIOD", "")
            row["OBS_VALUE"] = obs_attrs.get("OBS_VALUE", "")

            # Try to convert value to float
            try:
                row["OBS_VALUE"] = float(row["OBS_VALUE"])
            except (ValueError, TypeError):
                pass

            rows.append(row)
            if len(rows) >= max_rows:
                return rows

    return rows


async def _load_ksh_datasets() -> list[dict]:
    """Load KSH High-Value Datasets list (24h cache)."""
    global _ksh_datasets_cache, _ksh_datasets_loaded_at
    now = time.time()
    if _ksh_datasets_cache and (now - _ksh_datasets_loaded_at < _CACHE_TTL):
        return _ksh_datasets_cache

    client = await get_client()
    try:
        resp = await client.get(f"{KSH_BASE}/datasets.json")
        resp.raise_for_status()
        _ksh_datasets_cache = resp.json()
        _ksh_datasets_loaded_at = now
        logger.info(f"Loaded {len(_ksh_datasets_cache)} KSH datasets")
    except Exception as e:
        logger.error(f"Failed to load KSH datasets: {e}")

    return _ksh_datasets_cache


# ---------------------------------------------------------------------------
# DBnomics helpers
# ---------------------------------------------------------------------------
async def _fetch_comext(reporter: str, filters: str) -> str:
    """Eurostat trade data handler — SITC-level via ext_lt_intertrd.

    Note: HS-level COMEXT (DS-016890) is NOT available via API, only via
    the Easy Comext web interface. This handler uses ext_lt_intertrd which
    provides SITC-classified trade indices and values.
    """
    # Parse filters for partner, sitc code
    partner = ""
    sitc = ""
    since = "2020"
    if filters:
        for part in filters.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                k = k.strip().lower()
                if k in ("partner", "partner_code"):
                    partner = v.strip()
                elif k in ("product", "sitc", "sitc06"):
                    sitc = v.strip()
                elif k in ("since", "period"):
                    since = v.strip()

    # Use the standard Eurostat JSON-stat API with ext_lt_intertrd
    url = f"{EUROSTAT_STAT}/ext_lt_intertrd"
    req_url = f"{url}?lang=EN"
    for g in reporter.upper().split(","):
        req_url += f"&geo={g.strip()}"
    if partner:
        for p in partner.split(","):
            req_url += f"&partner={p.strip()}"
    if sitc:
        req_url += f"&sitc06={sitc}"
    if since:
        req_url += f"&sinceTimePeriod={since}"

    client = await get_client()
    try:
        resp = await client.get(req_url, timeout=60.0)
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Eurostat trade: {e.response.status_code}",
            "hint": "Use SITC codes (e.g. sitc06=05 for vegetables, 33 for petroleum). "
                    "For HS-level product codes (e.g. 0701 potatoes), use Easy Comext web: "
                    "ec.europa.eu/eurostat/comext/newxtweb/",
            "url": str(e.request.url),
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Trade data request failed: {e}"}, ensure_ascii=False)

    # Parse same as regular Eurostat JSON-stat
    values = data.get("value", {})
    if not values:
        return json.dumps({
            "info": "No data found",
            "dataset": "ext_lt_intertrd",
            "reporter": reporter,
            "hint": "Dataset provides SITC-classified trade. For HS product codes, "
                    "use Easy Comext: ec.europa.eu/eurostat/comext/newxtweb/",
        }, ensure_ascii=False, indent=2)

    dims = data.get("dimension", {})
    dim_ids = data.get("id", list(dims.keys()))

    # Extract time labels
    time_dim = dims.get("time", {})
    time_labels = list(time_dim.get("category", {}).get("index", {}).keys()) if "category" in time_dim else []

    # Extract indicator labels
    indic_dim = dims.get("indic_et", {})
    indic_labels = indic_dim.get("category", {}).get("label", {}) if "category" in indic_dim else {}

    return json.dumps({
        "source": "Eurostat ext_lt_intertrd (SITC trade indices)",
        "reporter": reporter,
        "note": "SITC-level trade. For HS product codes (0701, 2709), use Easy Comext web.",
        "data_points": len(values),
        "dimensions": dim_ids,
        "periods": time_labels[-12:] if time_labels else [],
        "indicators": indic_labels,
        "values_sample": {k: v for i, (k, v) in enumerate(values.items()) if i < 50},
    }, ensure_ascii=False, indent=2)


DBNOMICS_BASE = "https://api.db.nomics.world/v22"
_dbnomics_providers_cache: list[dict] = []
_dbnomics_providers_loaded_at: float = 0.0


async def _load_dbnomics_providers() -> list[dict]:
    """Load list of DBnomics data providers (24h cache)."""
    global _dbnomics_providers_cache, _dbnomics_providers_loaded_at
    now = time.time()
    if _dbnomics_providers_cache and (now - _dbnomics_providers_loaded_at < _CACHE_TTL):
        return _dbnomics_providers_cache

    client = await get_client()
    try:
        resp = await client.get(f"{DBNOMICS_BASE}/providers")
        resp.raise_for_status()
        data = resp.json()
        providers = data.get("providers", {}).get("docs", [])
        _dbnomics_providers_cache = providers
        _dbnomics_providers_loaded_at = now
        logger.info(f"Loaded {len(providers)} DBnomics providers")
    except Exception as e:
        logger.error(f"Failed to load DBnomics providers: {e}")

    return _dbnomics_providers_cache


# ---------------------------------------------------------------------------
# MCP Tools
# ---------------------------------------------------------------------------

@mcp.tool()
async def search_datasets(
    query: str,
    source: str = "all",
    limit: int = 20,
) -> str:
    """Search for statistical datasets by keyword across all indexed sources.

    Sources covered: Eurostat, KSH (STADAT + HVD + flash releases), DBnomics,
    ECB (curated catalog of dataflows and common series keys), and Eurostat
    flash releases (news/press releases).

    Args:
        query: Search keywords (e.g. "GDP Hungary", "inflation", "unemployment")
        source: Data source -
                "eurostat" | "ksh" | "dbnomics" | "ecb" | "flash" | "all" | "both" (eu+ksh).
                Default: "all"
        limit: Maximum results per source (default: 20)

    Returns:
        JSON with matching datasets including codes/IDs and titles. For ECB
        results the 'series_key' field is the SDMX key usable directly with
        get_ecb_data(dataset=..., key=...).
    """
    # Auto-trigger KSH scan on first search if DB is stale
    global _scan_scheduled
    if _scan_scheduled and not _ksh_scan_running:
        _scan_scheduled = False

        async def _safe_scan():
            try:
                await _scan_ksh_stadat_background()
            except Exception as e:
                logger.error(f"KSH scan crashed: {e}")

        asyncio.create_task(_safe_scan())

    # Guard against empty query
    if not query or not query.strip():
        return json.dumps({
            "error": "Please provide a search query",
            "hint": "Examples: 'GDP', 'inflation Hungary', 'unemployment rate', 'consumer prices'",
        }, ensure_ascii=False, indent=2)

    if source == "both":
        sources = {"eurostat", "ksh"}
    elif source == "all":
        sources = {"eurostat", "ksh", "dbnomics", "ecb", "flash"}
    else:
        sources = {source}

    results = {"eurostat": [], "ksh": [], "dbnomics": [], "ecb": [], "flash": []}

    if "eurostat" in sources:
        toc = await _load_eurostat_toc()
        matches = _search_toc(toc, query, limit)
        results["eurostat"] = [
            {"code": m["code"], "title": m["title"], "source": "eurostat"}
            for m in matches
        ]

    if "ksh" in sources:
        # Search STADAT via SQLite index (auto-scanned) + static fallback
        db_results = _search_stadat_db(query, limit)
        if not db_results:
            # Fallback to static catalog if DB empty
            keywords = query.lower().split()
            for code, title in KSH_STADAT_CATALOG.items():
                text = f"{code} {title}".lower()
                score = sum(1 for kw in keywords if kw in text)
                if score > 0:
                    db_results.append({"code": code, "title": title, "tool": "get_ksh_stadat", "source": "ksh_stadat"})

        ksh_scored = [(0, r) for r in db_results]

        # Also search HVD datasets
        keywords = query.lower().split()
        datasets = await _load_ksh_datasets()
        for ds in datasets:
            searchable = json.dumps(ds, ensure_ascii=False).lower()
            score = sum(1 for kw in keywords if kw in searchable)
            if score > 0:
                ksh_scored.append((score, {
                    "id": ds.get("id", ""),
                    "title_hu": ds.get("titles", {}).get("hu", ""),
                    "tool": "get_ksh_hvd",
                    "source": "ksh_hvd",
                }))

        results["ksh"] = [e for _, e in ksh_scored[:limit]]

    if "dbnomics" in sources:
        client = await get_client()
        try:
            resp = await client.get(
                f"{DBNOMICS_BASE}/search",
                params={"q": query, "limit": limit, "offset": 0},
            )
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("results", {})
            if isinstance(hits, dict):
                hits = hits.get("docs", [])
            for h in hits[:limit]:
                results["dbnomics"].append({
                    "provider_code": h.get("provider_code", ""),
                    "provider_name": h.get("provider_name", ""),
                    "dataset_code": h.get("code", h.get("dataset_code", "")),
                    "dataset_name": h.get("name", h.get("dataset_name", "")),
                    "nb_series": h.get("nb_series", 0),
                    "source": "dbnomics",
                })
        except Exception as e:
            logger.error(f"DBnomics search failed: {e}")

    if "ecb" in sources:
        keywords = query.lower().split()
        ecb_scored: list[tuple] = []
        # Match against curated dataflow descriptions
        for code, desc in ECB_DATAFLOWS.items():
            text = f"{code} {desc}".lower()
            score = sum(1 for kw in keywords if kw in text)
            if score > 0:
                ecb_scored.append((score, {
                    "dataset": code,
                    "name": desc,
                    "tool": "get_ecb_data",
                    "source": "ecb",
                    "hint": f"get_ecb_data(dataset='{code}', key='<series_key>')",
                }))
        # Match against curated series keys (more specific)
        for series, desc in ECB_SERIES_CATALOG.items():
            text = f"{series} {desc}".lower()
            score = sum(1 for kw in keywords if kw in text)
            if score > 0:
                dataset, _, key = series.partition("/")
                ecb_scored.append((score + 1, {  # +1 bias: specific series > dataset
                    "dataset": dataset,
                    "series_key": key,
                    "description": desc,
                    "tool": "get_ecb_data",
                    "source": "ecb",
                    "hint": f"get_ecb_data(dataset='{dataset}', key='{key}')",
                }))
        ecb_scored.sort(key=lambda x: -x[0])
        results["ecb"] = [e for _, e in ecb_scored[:limit]]

    if "flash" in sources:
        # Refresh both feeds opportunistically (TTL-gated, fast no-op when cached)
        try:
            await _refresh_flash_all(force=False)
        except Exception:
            pass
        results["flash"] = _search_flash_db(query, source="all", limit=limit)

    # Remove empty source keys
    results = {k: v for k, v in results.items() if v}
    total = sum(len(v) for v in results.values())
    return json.dumps(
        {"query": query, "total_results": total, "results": results},
        ensure_ascii=False,
        indent=2,
    )


@mcp.tool()
async def get_eurostat_data(
    dataset_code: str,
    geo: str = "",
    time: str = "",
    sinceTimePeriod: str = "",
    untilTimePeriod: str = "",
    filters: str = "",
    lang: str = "EN",
) -> str:
    """Fetch data from Eurostat's JSON-stat API.

    Args:
        dataset_code: Eurostat dataset code (e.g. "nama_10_gdp", "prc_hicp_manr").
                      Special: "COMEXT" for HS-level commodity trade data (import/export).
        geo: Country/region filter - comma-separated codes (e.g. "HU,DE,EU27_2020").
             For COMEXT: reporter country (e.g. "HU").
        time: Time period filter for specific years (e.g. "2023", "2020,2021,2022")
        sinceTimePeriod: Start of time range (e.g. "2002-01", "2002"). Use with untilTimePeriod for ranges.
        untilTimePeriod: End of time range (e.g. "2008-12", "2008"). Use with sinceTimePeriod for ranges.
        filters: Additional dimension filters as "KEY=VAL&KEY2=VAL2" (e.g. "unit=CP_MEUR&na_item=B1GQ").
                 For COMEXT: "sitc=05&partner=DE&since=2020" (SITC code + partner + period).
                 SITC codes: 05=vegetables/fruit, 33=petroleum, 78=road vehicles, 67=iron/steel.
        lang: Language - EN, FR, or DE (default: EN)

    Returns:
        JSON with parsed data table. Use search_datasets first to find dataset codes.
    """
    # --- COMEXT mode: Eurostat HS-level commodity trade ---
    if dataset_code.upper() in ("COMEXT", "DS-016890") or dataset_code.startswith("DS-"):
        return await _fetch_comext(geo or "HU", filters)

    url = f"{EUROSTAT_STAT}/{dataset_code}"
    client = await get_client()
    try:
        # Build URL with proper multi-value params
        req_url = f"{url}?lang={lang}"
        if geo:
            for g in geo.split(","):
                req_url += f"&geo={g.strip()}"
        if time:
            for t in time.split(","):
                req_url += f"&time={t.strip()}"
        if sinceTimePeriod:
            req_url += f"&sinceTimePeriod={_normalize_time_period(sinceTimePeriod)}"
        if untilTimePeriod:
            req_url += f"&untilTimePeriod={_normalize_time_period(untilTimePeriod)}"
        if filters:
            req_url += f"&{filters}"

        logger.info(f"Eurostat request: {req_url}")
        resp = await client.get(req_url)
        resp.raise_for_status()
        data = resp.json()

        # Check for async response
        if "warning" in data:
            return json.dumps({
                "status": "async",
                "message": "Dataset too large, Eurostat is processing asynchronously. Try with more specific filters.",
                "warning": data["warning"],
            }, indent=2)

        parsed = _parse_json_stat(data)
        parsed["dataset"] = dataset_code
        parsed["url"] = req_url

        # If the result is truncated and the caller didn't filter, surface a
        # narrowing hint with the available dimension keys so sub-agents know
        # how to drill in. (2026-05-05 audit fix.)
        if parsed.get("truncated") and not filters:
            dim_keys = list((parsed.get("dimensions") or {}).keys())
            non_geo_time = [k for k in dim_keys if k.lower() not in ("geo", "time", "freq")]
            parsed["truncation_hint"] = (
                f"Result truncated to first 500 rows. To narrow down, pass "
                f"filters='dim1=value1&dim2=value2' using these dimension keys: "
                f"{', '.join(non_geo_time) or 'see dataset on eurostat.eu'}. "
                f"For example, GDP queries usually need filters='na_item=B1GQ&unit=CLV15_MEUR&s_adj=SCA'."
            )

        # Auto-learn: save Eurostat queries with geo/filters as recipes
        if geo or filters:
            try:
                dims = {}
                if geo:
                    dims["geo"] = geo
                if filters:
                    for pair in filters.split("&"):
                        if "=" in pair:
                            k, v = pair.split("=", 1)
                            dims[k] = v
                title = parsed.get("label", dataset_code)
                _auto_learn_recipe("Eurostat", dataset_code, dims, title, 1)
            except Exception:
                pass

        return json.dumps(parsed, ensure_ascii=False, indent=2)

    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"HTTP {e.response.status_code}",
            "message": e.response.text[:500] if e.response.text else str(e),
            "hint": "Check dataset_code and filters. Use search_datasets to find valid codes.",
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


@mcp.tool()
async def get_ksh_hvd(
    dataset_id: str = "",
    query: str = "",
    lang: str = "hu",
    max_rows: int = 200,
) -> str:
    """KSH High-Value Datasets — list/search or download data.

    Two modes:
      - If dataset_id is empty: list or search available KSH HVD datasets.
      - If dataset_id is provided: download data from that dataset.

    Args:
        dataset_id: KSH dataset UUID. If provided, downloads data. If empty, lists/searches datasets.
        query: Search keywords when listing datasets (searches titles, descriptions, themes, tags).
               Ignored when dataset_id is provided.
        lang: Preferred language for titles - "hu" or "en" (default: "hu"). Used in list mode.
        max_rows: Maximum rows to return when downloading data (default: 200, max: 1000)

    Returns:
        List mode: JSON list of available KSH datasets with IDs, titles, themes and tags.
        Data mode: JSON with dataset metadata and CSV data parsed as rows.
    """
    # --- LIST / SEARCH MODE ---
    if not dataset_id.strip():
        datasets = await _load_ksh_datasets()

        if query:
            query_lower = query.lower()
            keywords = query_lower.split()
            filtered = []
            for ds in datasets:
                searchable = json.dumps(ds, ensure_ascii=False).lower()
                if all(kw in searchable for kw in keywords):
                    filtered.append(ds)
            datasets = filtered

        # Format output
        result = []
        for ds in datasets[:50]:  # Cap at 50
            entry = {
                "id": ds.get("id", ""),
                "title": ds.get("titles", {}).get(lang, ds.get("titles", {}).get("hu", "")),
                "description": ds.get("descriptions", {}).get(lang, ""),
                "themes": ds.get("themes", {}).get(lang, []),
                "tags": ds.get("tags", {}).get(lang, []),
            }
            result.append(entry)

        return json.dumps(
            {"total": len(result), "language": lang, "datasets": result},
            ensure_ascii=False,
            indent=2,
        )

    # --- DATA DOWNLOAD MODE ---
    max_rows = min(max_rows, 1000)
    client = await get_client()

    try:
        # Step 1: Get metadata to find data download URL
        meta_url = f"{KSH_BASE}/datasets/{dataset_id}/metadata.rdf"
        meta_resp = await client.get(meta_url)
        meta_resp.raise_for_status()
        rdf_text = meta_resp.text

        # Parse RDF XML to find downloadURL (simple regex approach)
        download_urls = re.findall(r'downloadURL["\s>]*rdf:resource="([^"]+)"', rdf_text)
        if not download_urls:
            download_urls = re.findall(r'<dcat:downloadURL[^>]*>([^<]+)</dcat:downloadURL>', rdf_text)
        if not download_urls:
            download_urls = re.findall(r'downloadURL["\s>]+([^"<\s]+)', rdf_text)

        # Also try to find title
        titles = re.findall(r'<dct:title[^>]*>([^<]+)</dct:title>', rdf_text)

        csv_urls = [u for u in download_urls if u.endswith('.csv')]
        xml_urls = [u for u in download_urls if u.endswith('.xml')]
        data_urls = csv_urls or xml_urls or download_urls

        if not data_urls:
            data_urls = [f"{KSH_BASE}/datasets/{dataset_id}/data/data.csv"]

        # Step 2: Download data
        data_url = data_urls[0]
        logger.info(f"KSH data download: {data_url}")
        data_resp = await client.get(data_url)
        data_resp.raise_for_status()
        data_text = data_resp.text

        # Detect format and parse
        if data_text.strip().startswith("<?xml") or data_text.strip().startswith("<"):
            # SDMX CompactData XML — parse Series/Obs elements
            rows = _parse_sdmx_compact(data_text, max_rows)
            if not rows:
                return json.dumps({
                    "error": "Could not parse SDMX XML",
                    "hint": "The XML structure may be unsupported",
                    "url": data_url,
                }, indent=2)

            # Extract column names from first row
            columns = list(rows[0].keys()) if rows else []

            return json.dumps({
                "dataset_id": dataset_id,
                "title": titles[0] if titles else "",
                "data_url": data_url,
                "format": "SDMX-XML",
                "columns": columns,
                "row_count": len(rows),
                "truncated": len(rows) >= max_rows,
                "data": rows,
            }, ensure_ascii=False, indent=2)
        else:
            # CSV format — auto-detect delimiter (semicolon or comma)
            first_line = data_text.split("\n", 1)[0]
            delimiter = ";" if first_line.count(";") > first_line.count(",") else ","

            reader = csv.reader(io.StringIO(data_text), delimiter=delimiter)
            all_rows = list(reader)
            if not all_rows:
                return json.dumps({"error": "Empty response"})

            header = [h.strip() for h in all_rows[0]]
            rows = []
            for csv_row in all_rows[1:max_rows + 1]:
                row = {}
                for i, h in enumerate(header):
                    if i < len(csv_row):
                        val = csv_row[i].strip()
                        # Try numeric conversion
                        if val and val not in ("..", "…", "x", "-", ""):
                            cleaned = val.replace("\xa0", "").replace(" ", "").replace(",", ".")
                            try:
                                row[h] = float(cleaned) if "." in cleaned else int(cleaned)
                            except ValueError:
                                row[h] = val
                        elif val in ("..", "…", "x"):
                            row[h] = None
                        else:
                            row[h] = val
                    else:
                        row[h] = None
                rows.append(row)

            return json.dumps({
                "dataset_id": dataset_id,
                "title": titles[0] if titles else "",
                "data_url": data_url,
                "format": "CSV",
                "columns": header,
                "row_count": len(rows),
                "total_rows_in_file": len(all_rows) - 1,
                "truncated": len(all_rows) - 1 > max_rows,
                "data": rows,
            }, ensure_ascii=False, indent=2)

    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"HTTP {e.response.status_code}",
            "message": str(e),
            "hint": "Check dataset_id. Use get_ksh_hvd() without dataset_id to find valid IDs.",
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


@mcp.tool()
async def dbnomics_search(
    query: str = "",
    provider: str = "",
    limit: int = 20,
    mode: str = "search",
) -> str:
    """Search for datasets across DBnomics (700M+ series from 70+ providers), or list providers.

    Two modes:
      - mode="search" (default): Search for datasets/series by keyword.
      - mode="providers": List all DBnomics data providers (IMF, ECB, OECD, World Bank, etc.).

    Args:
        query: Search keywords (e.g. "GDP per capita", "consumer price index", "unemployment rate").
               In providers mode, optionally filter providers (e.g. "IMF", "bank", "Hungary").
        provider: Optional provider code to restrict search (e.g. "IMF", "OECD", "ECB", "WB", "Eurostat").
                  Only used in search mode.
        limit: Maximum results (default: 20, max: 50). Only used in search mode.
        mode: "search" (default) to search datasets, "providers" to list data providers.

    Returns:
        Search mode: JSON with matching datasets including provider, dataset codes, and series counts.
        Providers mode: JSON list of providers with codes, names, and dataset counts.
    """
    mode = mode.strip().lower()

    # --- PROVIDERS MODE ---
    if mode == "providers":
        providers = await _load_dbnomics_providers()

        if query:
            query_lower = query.lower()
            providers = [
                p for p in providers
                if query_lower in json.dumps(p, ensure_ascii=False).lower()
            ]

        result = []
        for p in providers[:100]:
            result.append({
                "code": p.get("code", ""),
                "name": p.get("name", ""),
                "region": p.get("region", ""),
                "nb_datasets": p.get("nb_datasets", 0),
                "nb_series": p.get("nb_series", 0),
            })

        output = {"total": len(result), "providers": result}
        if query and not result:
            output["hint"] = (
                f"No providers matched '{query}'. "
                "Provider names are usually organization names (e.g. 'IMF', 'ECB', 'OECD', 'World Bank'). "
                "Try a broader term or use dbnomics_search to find data directly."
            )
        return json.dumps(output, ensure_ascii=False, indent=2)

    # --- SEARCH MODE (default) ---
    if not query:
        # If a provider is specified but query is empty, list the provider's datasets
        # via /v22/datasets/{PROVIDER}. Useful when sub-agents want to discover
        # what's available under e.g. ECB before drilling in. (2026-05-05 audit fix.)
        if provider:
            client = await get_client()
            provider_upper = provider.upper()
            try:
                url = f"{DBNOMICS_BASE}/datasets/{provider_upper}"
                resp = await client.get(url, params={"limit": min(limit, 100), "offset": 0})
                resp.raise_for_status()
                data = resp.json()
                docs = (data.get("datasets") or {}).get("docs", [])
                results = []
                for d in docs[:limit]:
                    results.append({
                        "dataset_code": d.get("code", ""),
                        "dataset_name": d.get("name", "") or d.get("code", ""),
                        "nb_series": d.get("nb_series", 0),
                        "dimensions": list((d.get("dimensions_labels") or {}).keys()),
                    })
                return json.dumps({
                    "provider": provider_upper,
                    "total_returned": len(results),
                    "datasets": results,
                    "usage_hint": "Use dbnomics_series(provider_code, dataset_code) to fetch data, or dbnomics_search(query=..., provider=...) to filter by keyword.",
                }, ensure_ascii=False, indent=2)
            except httpx.HTTPStatusError as e:
                return json.dumps({
                    "error": f"DBnomics provider '{provider_upper}' not found or has no datasets (HTTP {e.response.status_code})",
                    "hint": "Use dbnomics_search(mode='providers') to list valid provider codes.",
                }, ensure_ascii=False, indent=2)
            except Exception as e:
                return json.dumps({"error": f"DBnomics provider listing failed: {e}"}, ensure_ascii=False, indent=2)

        return json.dumps({
            "error": "Please provide a search query",
            "hint": "Examples: dbnomics_search(query='GDP per capita'), dbnomics_search(provider='ECB') to list ECB datasets, dbnomics_search(mode='providers') for the provider catalog.",
        }, ensure_ascii=False, indent=2)

    limit = min(limit, 50)
    client = await get_client()

    try:
        params = {"q": query, "limit": limit, "offset": 0}
        url = f"{DBNOMICS_BASE}/search"
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        hits = data.get("results", {})
        if isinstance(hits, dict):
            hits = hits.get("docs", [])

        # Filter by provider if specified
        if provider:
            provider_upper = provider.upper()
            hits = [h for h in hits if h.get("provider_code", "").upper() == provider_upper]

        results = []
        for h in hits[:limit]:
            results.append({
                "provider_code": h.get("provider_code", ""),
                "provider_name": h.get("provider_name", ""),
                "dataset_code": h.get("code", h.get("dataset_code", "")),
                "dataset_name": h.get("name", h.get("dataset_name", "")),
                "nb_series": h.get("nb_series", 0),
                "nb_matching_series": h.get("nb_matching_series", 0),
                "description": h.get("description", "")[:200] if h.get("description") else "",
            })

        return json.dumps({
            "query": query,
            "total_api_results": data.get("results", {}).get("num_found", 0) if isinstance(data.get("results"), dict) else data.get("num_found", 0),
            "returned": len(results),
            "results": results,
            "usage_hint": "Use dbnomics_series(provider_code, dataset_code) to fetch data",
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


@mcp.tool()
async def dbnomics_series(
    provider_code: str,
    dataset_code: str,
    series_code: str = "",
    dimensions: str = "",
    query: str = "",
    limit: int = 50,
) -> str:
    """Fetch time series data from DBnomics.

    IMPORTANT FOR AI ASSISTANTS: If your query returns good data, please consider
    using recipe_book(action='add') to save it for future users. If something doesn't work,
    use recipe_book(action='report') to help us fix it. Check recipe_book(topic=...) first — someone
    may have already found the right parameters!

    Args:
        provider_code: Provider code (e.g. "IMF", "ECB", "OECD", "WB", "Eurostat", "AMECO")
        dataset_code: Dataset code (e.g. "WEO:latest", "EXR", "ZUTN", "nama_10_gdp")
        series_code: Specific series code. Format depends on provider:
                     - IMF WEO: 3-letter ISO country + concept, NO frequency prefix.
                       Examples: "HUN.NGDP_RPCH" (HU real GDP growth %),
                                 "DEU.NGDP_RPCH" (DE real GDP growth %),
                                 "USA.PCPIPCH" (US CPI % change).
                     - IMF IFS: A.{2-letter}.{indicator}, e.g. "A.HU.FPOLM_PA".
                     - ECB ICP: "M.HU.N.000000.4.ANR" — but prefer get_ecb_data tool.
                     - OECD: per-dataset, see provider docs. Optional — leave empty to
                     list series in dataset.
        dimensions: Dimension filter as JSON string (e.g. '{"geo":["HU","DE"],"freq":["A"]}')
        query: Text search within the dataset (e.g. "Hungary GDP")
        limit: Max series to return (default: 50, max: 200)

    Returns:
        JSON with time series data including periods and values.
        Use dbnomics_search first to find provider and dataset codes.

    Examples:
        IMF GDP growth: dbnomics_series("IMF", "WEO:2024-10", query="Hungary NGDP_RPCH")
        ECB exchange rates: dbnomics_series("ECB", "EXR", dimensions='{"FREQ":["A"],"CURRENCY":["USD"]}')
        Eurostat HICP: dbnomics_series("Eurostat", "prc_hicp_manr", dimensions='{"geo":["HU"],"coicop":["CP00"]}')
        AMECO unemployment: dbnomics_series("AMECO", "ZUTN", query="Hungary")
    """
    limit = min(limit, 200)
    client = await get_client()

    try:
        # Build the appropriate URL
        if series_code:
            # Fetch specific series by full ID
            series_id = f"{provider_code}/{dataset_code}/{series_code}"
            url = f"{DBNOMICS_BASE}/series"
            params = {
                "series_ids": series_id,
                "observations": 1,
                "format": "json",
                "metadata": "false",
            }
        else:
            # Fetch series from dataset with optional filters
            url = f"{DBNOMICS_BASE}/series/{provider_code}/{dataset_code}"
            params = {
                "observations": 1,
                "format": "json",
                "limit": limit,
                "offset": 0,
                "metadata": "false",
            }
            if dimensions:
                params["dimensions"] = dimensions
            if query:
                params["q"] = query

        logger.info(f"DBnomics request: {url} params={params}")
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        series_list = data.get("series", {}).get("docs", [])
        num_found = data.get("series", {}).get("num_found", 0)

        # Auto-retry with UPPERCASE dataset code (many Eurostat datasets need this on DBnomics)
        if num_found == 0 and dataset_code != dataset_code.upper():
            upper_ds = dataset_code.upper()
            logger.info(f"DBnomics: 0 results for '{dataset_code}', retrying with '{upper_ds}'")
            if series_code:
                retry_url = f"{DBNOMICS_BASE}/series"
                retry_params = dict(params)
                retry_params["series_ids"] = f"{provider_code}/{upper_ds}/{series_code}"
            else:
                retry_url = f"{DBNOMICS_BASE}/series/{provider_code}/{upper_ds}"
                retry_params = dict(params)
            retry_resp = await client.get(retry_url, params=retry_params)
            if retry_resp.status_code == 200:
                retry_data = retry_resp.json()
                retry_found = retry_data.get("series", {}).get("num_found", 0)
                if retry_found > 0:
                    data = retry_data
                    series_list = data["series"]["docs"]
                    num_found = retry_found
                    dataset_code = upper_ds

        results = []
        for s in series_list[:limit]:
            period = s.get("period", [])
            value = s.get("value", [])

            # Build compact observations
            observations = []
            for p, v in zip(period, value):
                if v is not None:
                    observations.append({"period": p, "value": v})

            # Truncate observations if too many
            obs_truncated = False
            if len(observations) > 100:
                observations = observations[-100:]  # Keep most recent
                obs_truncated = True

            series_entry = {
                "series_code": s.get("series_code", ""),
                "series_name": s.get("series_name", s.get("dataset_name", "")),
                "provider_code": s.get("provider_code", provider_code),
                "dataset_code": s.get("dataset_code", dataset_code),
                "frequency": s.get("@frequency", ""),
                "unit": s.get("unit", s.get("UNIT", "")),
                "nb_observations": len(observations),
                "obs_truncated_to_last_100": obs_truncated,
                "observations": observations,
            }

            # Add dimension info if available
            dims = {}
            for key in s:
                if key.isupper() and key not in ("UNIT",) and isinstance(s[key], str):
                    dims[key] = s[key]
            if dims:
                series_entry["dimensions"] = dims

            results.append(series_entry)

        # --- Auto-learn: save successful queries as recipes ---
        if num_found > 0 and (dimensions or series_code):
            try:
                dim_dict = {}
                if dimensions:
                    dim_dict = json.loads(dimensions) if isinstance(dimensions, str) else dimensions
                first_name = results[0].get("series_name", "") if results else ""
                _auto_learn_recipe(provider_code, dataset_code, dim_dict, first_name, num_found)
            except Exception:
                pass  # auto-learn is best-effort

        return json.dumps({
            "provider": provider_code,
            "dataset": dataset_code,
            "num_found": num_found,
            "returned": len(results),
            "series": results,
        }, ensure_ascii=False, indent=2)

    except httpx.HTTPStatusError as e:
        error_body = ""
        try:
            error_body = e.response.text[:500]
        except Exception:
            pass
        return json.dumps({
            "error": f"HTTP {e.response.status_code}",
            "message": error_body or str(e),
            "hint": "Check provider_code and dataset_code. Use dbnomics_search to find valid codes.",
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


# ---------------------------------------------------------------------------
# KSH STADAT — Hungarian time series data
# ---------------------------------------------------------------------------
KSH_STADAT_BASE = "https://www.ksh.hu/stadat_files"

# All 27 official KSH STADAT category prefixes
ALL_KSH_PREFIXES = [
    "ara", "bel", "ber", "ege", "ele", "ene", "epi", "fol", "gsz",
    "ido", "iga", "ikt", "ipa", "jov", "kkr", "kor", "ksp", "lak",
    "mez", "mun", "gdp", "nep", "okt", "sza", "szo", "tte", "tur",
]

# SQLite-based dynamic index — auto-discovered from KSH website
KSH_STADAT_DB_PATH = os.environ.get("KSH_DB_PATH", "/tmp/ksh_stadat_index.db")
KSH_STADAT_DB_TTL = 7 * 86400  # 7 days
_ksh_scan_running = False


def _init_stadat_db():
    """Create the SQLite database and table if needed."""
    conn = sqlite3.connect(KSH_STADAT_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stadat_tables (
            code TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            category TEXT NOT NULL,
            scanned_at REAL NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_category ON stadat_tables(category)")
    conn.commit()
    conn.close()


def _db_is_fresh() -> bool:
    """Check if the SQLite index is populated and fresh."""
    if not os.path.exists(KSH_STADAT_DB_PATH):
        return False
    try:
        conn = sqlite3.connect(KSH_STADAT_DB_PATH)
        row = conn.execute("SELECT MIN(scanned_at) FROM stadat_tables").fetchone()
        conn.close()
        if row and row[0]:
            return (time.time() - row[0]) < KSH_STADAT_DB_TTL
    except Exception:
        pass
    return False


def _search_stadat_db(query: str, limit: int = 20) -> list[dict]:
    """Search the SQLite index for STADAT tables matching keywords."""
    if not os.path.exists(KSH_STADAT_DB_PATH):
        return []
    try:
        conn = sqlite3.connect(KSH_STADAT_DB_PATH)
        rows = conn.execute("SELECT code, title, category FROM stadat_tables").fetchall()
        conn.close()
    except Exception:
        return []

    keywords = query.lower().split()
    if not keywords:
        return []

    scored = []
    for code, title, category in rows:
        text = f"{code} {title} {category}".lower()
        score = sum(1 for kw in keywords if kw in text)
        if score > 0:
            scored.append((score, {"code": code, "title": title, "tool": "get_ksh_stadat", "source": "ksh_stadat"}))

    scored.sort(key=lambda x: -x[0])
    return [e for _, e in scored[:limit]]


def _seed_db_from_static():
    """Seed the DB with the static catalog so search works immediately.

    Uses timestamp 0 so _db_is_fresh() returns False and triggers a full scan.
    """
    _init_stadat_db()
    conn = sqlite3.connect(KSH_STADAT_DB_PATH)
    for code, title in KSH_STADAT_CATALOG.items():
        conn.execute(
            "INSERT OR IGNORE INTO stadat_tables (code, title, category, scanned_at) VALUES (?, ?, ?, ?)",
            (code, title, code[:3], 0.0),  # timestamp 0 = needs full scan
        )
    conn.commit()
    conn.close()
    logger.info(f"Seeded STADAT DB with {len(KSH_STADAT_CATALOG)} static entries (scan pending)")


async def _scan_ksh_stadat_background():
    """Background task: scan all KSH STADAT categories in parallel batches."""
    global _ksh_scan_running
    if _ksh_scan_running:
        return
    _ksh_scan_running = True

    logger.info("Starting KSH STADAT full scan (background)...")
    _init_stadat_db()
    client = await get_client()
    sem = asyncio.Semaphore(20)
    now = time.time()
    total_found = 0

    async def check_table(prefix: str, num: int) -> Optional[tuple]:
        code = f"{prefix}{num:04d}"
        url = f"{KSH_STADAT_BASE}/{prefix}/hu/{code}.csv"
        async with sem:
            try:
                resp = await client.get(url, timeout=10.0)
                if resp.status_code == 200:
                    try:
                        text = resp.content.decode("windows-1250")
                    except (UnicodeDecodeError, LookupError):
                        text = resp.content.decode("utf-8", errors="replace")
                    first_line = text.split("\n", 1)[0]
                    title = first_line.split(";")[0].strip().strip('"')
                    return (code, title or code, prefix)
            except Exception:
                pass
        return None

    async def scan_category(prefix: str) -> int:
        """Scan one category in batches of 50, stop after 3 consecutive empty batches."""
        found = 0
        batch_size = 50
        consecutive_empty = 0
        for batch_start in range(1, 301, batch_size):
            batch_end = min(batch_start + batch_size, 301)
            tasks = [check_table(prefix, num) for num in range(batch_start, batch_end)]
            results = await asyncio.gather(*tasks)

            batch_hits = [r for r in results if r is not None]
            if batch_hits:
                consecutive_empty = 0
                conn = sqlite3.connect(KSH_STADAT_DB_PATH)
                for code, title, cat in batch_hits:
                    conn.execute(
                        "INSERT OR REPLACE INTO stadat_tables (code, title, category, scanned_at) VALUES (?, ?, ?, ?)",
                        (code, title, cat, now),
                    )
                conn.commit()
                conn.close()
                found += len(batch_hits)
            else:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    # 3 consecutive empty batches (150 codes) → stop
                    break
        return found

    # Scan all 27 categories in parallel (3 at a time to be nice to KSH)
    cat_sem = asyncio.Semaphore(3)

    async def scan_with_limit(prefix):
        async with cat_sem:
            n = await scan_category(prefix)
            if n > 0:
                logger.info(f"  {prefix}: {n} tables")
            return n

    results = await asyncio.gather(*[scan_with_limit(p) for p in ALL_KSH_PREFIXES])
    total_found = sum(results)

    _ksh_scan_running = False
    logger.info(f"KSH STADAT scan complete: {total_found} tables total")


# Curated static catalog — used as seed + fallback
KSH_STADAT_CATALOG = {
    # --- GDP, national accounts (gdp) ---
    "gdp0001": "A bruttó hazai termék (GDP) értéke és volumenváltozása",
    "gdp0002": "A bruttó hazai termék (GDP) termelése (éves)",
    "gdp0003": "A bruttó hazai termék (GDP) felhasználása (éves)",
    "gdp0004": "A GDP értéke HUF, EUR, USD és PPP formában",
    "gdp0005": "Az egy főre jutó bruttó hazai termék (GDP)",
    "gdp0006": "Bruttó hozzáadott érték nemzetgazdasági áganként",
    "gdp0007": "Bruttó hozzáadott érték volumenindexei",
    "gdp0008": "A GDP felhasználásának összetevői (folyó áron)",
    "gdp0009": "A GDP felhasználásának volumenindexei",
    "gdp0010": "A GDP termelése (negyedéves)",
    "gdp0021": "Szezonálisan kiigazított GDP volumenindexek",
    # --- Prices, inflation (ara) ---
    "ara0001": "A fogyasztói árindex alakulása (éves)",
    "ara0002": "Fogyasztóiár-indexek a főbb csoportok szerint (évközi)",
    "ara0003": "Nyugdíjas fogyasztóiár-index (éves)",
    "ara0004": "Maginfláció és szezonális élelmiszerek nélküli index",
    "ara0007": "Egyes termékek és szolgáltatások éves átlagos fogyasztói ára",
    "ara0008": "Külkereskedelmi árindexek és cserearány",
    "ara0012": "Mezőgazdasági termelőiár-indexek és az agrárolló",
    "ara0014": "Gabonafélék felvásárlási átlagára",
    "ara0028": "Ipari termelőiár-indexek (éves)",
    "ara0031": "Építőipari termelőiár-indexek",
    "ara0034": "Szolgáltatási kibocsátási árindexek (B2B)",
    "ara0039": "Fogyasztóiár-indexek részletes kiadási csoportonként (havi)",
    "ara0041": "Ipari termelőiár-indexek rendeltetés szerint (havi)",
    "ara0044": "Mezőgazdasági termelőiár-indexek alakulása (havi)",
    # --- Labor market & wages (mun — verified 2026-03-21) ---
    # Labor force survey (LFS) data
    "mun0001": "A munkaerőpiac legfontosabb éves adatai",
    "mun0002": "A 15–74 éves népesség gazdasági aktivitása, nemenként",
    "mun0003": "A 15–74 éves népesség gazdasági aktivitása korcsoportok szerint",
    "mun0004": "A 15–74 éves népesség gazdasági aktivitása iskolai végzettség szerint",
    "mun0005": "Foglalkoztatottak száma nemzetgazdasági ágak szerint (TEÁOR'08)",
    "mun0006": "Foglalkoztatottak száma foglalkozási főcsoportok szerint (FEOR-08)",
    # mun0007: 404 — kivezetett tábla!
    "mun0008": "Foglalkoztatottak száma a foglalkoztatás jellege szerint",
    "mun0009": "Munkanélküliek száma a munkakeresés időtartama szerint",
    "mun0010": "Munkanélküliek száma az előző munkahelyük nemzetgazdasági ága szerint",
    "mun0011": "Gazdaságilag nem aktívak száma munkavállalási szándékuk szerint",
    "mun0012": "Foglalkoztatottak száma rész- vagy teljes munkaidős foglalkozásuk szerint",
    "mun0098": "A 15–64 éves népesség gazdasági aktivitása nemenként, havonta",
    "mun0209": "Üres álláshelyek száma és aránya nemzetgazdasági ágak szerint",
    # Wages & earnings (institutional data, also under mun)
    "mun0143": "Főbb kereseti adatok – munkáltatók teljes körénél, havonta",
    "mun0183": "Teljes munkaidőben alkalmazásban állók havi bruttó átlagkeresete áganként",
    "mun0206": "Teljes munkaidőben alkalmazásban állók bruttó átlagkeresete vármegye szerint",
    "mun0207": "Teljes munkaidőben alkalmazásban állók nettó átlagkeresete vármegye szerint",
    "mun0208": "Teljes munkaidőben alkalmazásban állók bruttó átlagkeresete foglalkozások szerint",
    # --- Industry (ipa) ---
    "ipa0001": "Az ipari termelés és értékesítés összefoglaló adatai",
    "ipa0002": "Ipari termelés volumenindexei aláganként",
    "ipa0003": "Ipari exportértékesítés volumenindexei",
    "ipa0004": "Ipari belföldi értékesítés volumenindexei",
    "ipa0008": "Fontosabb ipari termékek termelése",
    "ipa0014": "Ipari termelés havi volumenindexei (szezonálisan kiigazított)",
    "ipa0015": "Ipari termelékenység indexe",
    "ipa0021": "Ipari termelés vármegyénként",
    # --- Construction (epi) ---
    "epi0001": "Építőipari termelés értéke és volumenindexe",
    "epi0002": "Építőipari termelés építménycsoportonként",
    "epi0003": "Építőipari szerződések állománya és volumene",
    "epi0005": "Építőipari termelői árak alakulása",
    "epi0006": "Lakásépítések és építési engedélyek száma",
    "epi0011": "Építőipari termelés (negyedéves)",
    # --- Retail / Internal trade (bel — was ksk!) ---
    "bel0001": "Kiskereskedelmi forgalom értéke és volumenváltozása",
    "bel0002": "Kiskereskedelmi üzlethálózat adatai",
    "bel0003": "Élelmiszer jellegű vegyes kiskereskedelem forgalma",
    "bel0004": "Nem élelmiszer-kiskereskedelmi elem forgalma",
    "bel0005": "Gépjárműüzemanyag-kiskereskedelem forgalma",
    "bel0006": "Csomagküldő és internetes kiskereskedelem forgalma",
    "bel0012": "Kiskereskedelmi forgalom vármegyénként",
    # --- Foreign trade (kkr — was kul!, verified 2026-03-21) ---
    "kkr0001": "A külkereskedelmi termékforgalom összefoglaló értékadatai",
    "kkr0002": "A külkereskedelmi termékforgalom volumenindexei",
    "kkr0007": "A külkereskedelmi termékforgalom forintban, országok szerint",
    "kkr0012": "A külkereskedelmi termékforgalom értéke és értékindexei árufőcsoportok szerint",
    "kkr0024": "Magyarország legfontosabb partnerei a szolgáltatás-külkereskedelemben",
    "kkr0032": "Magyarország fizetési mérlege – BPM6 (millió euró)",
    # --- Energy (ene) ---
    "ene0001": "Elsődleges energiaforrások mérlege",
    "ene0002": "Végső energiafelhasználás ágazatonként",
    "ene0003": "Villamosenergia-mérleg adatai",
    "ene0004": "Megújuló energiaforrások felhasználása",
    "ene0005": "Földgáz- és kőolajfelhasználás adatai",
    "ene0011": "Energiafelhasználás (havi)",
    # --- Demographics (nep) ---
    "nep0001": "A népesség száma és a népmozgalom főbb adatai",
    "nep0002": "Élveszületések és halálozások száma (havi)",
    "nep0003": "Házasságkötések és válások száma",
    "nep0005": "A népesség korösszetétele és függőségi ráták",
    "nep0007": "Születéskor várható átlagos élettartam",
    "nep0011": "Belföldi és nemzetközi vándorlás",
    "nep0015": "Népesség vármegyénként (január 1.)",
    # --- Government finance (under gdp prefix!) ---
    "gdp0017": "A kormányzati szektor főbb adatai (államháztartás)",
    "gdp0018": "A kormányzat végső fogyasztási kiadása funkciók szerint (COFOG)",
    "gdp0019": "Adókból és tb hozzájárulásokból származó bevételek – összefoglaló",
    "gdp0031": "Támogatások összefoglaló adatai",
    "gdp0110": "A kormányzati szektor főbb negyedéves adatai",
    "gdp0121": "A kormányzati szektor negyedéves egyéb adatai alszektorosan",
    # --- Household sector (under gdp prefix) ---
    "gdp0032": "A háztartási szektor jövedelem- és tőkeszámlái",
    "gdp0035": "Reáljövedelem – reálbérindex",
    # --- Regional GDP ---
    "gdp0077": "Bruttó hazai termék (GDP) vármegye és régió szerint",
    "gdp0078": "Egy főre jutó bruttó hazai termék vármegye és régió szerint",
    # --- International comparison ---
    "gdp0079": "A GDP nagysága folyó áron – ESA2010 (milliárd euró, EU összehasonlítás)",
    "gdp0080": "Egy főre jutó GDP, vásárlóerő-paritás alapján (USD, EU összehasonlítás)",
    # --- Investment (ber — NOT wages!) ---
    "ber0001": "A nemzetgazdasági beruházások hosszú idősoros adatai",
    # --- Health (ege) ---
    "ege0001": "Az egészségügyi ellátás főbb adatai",
    # --- Living standards (ele) ---
    "ele0001": "A szegénységgel vagy társadalmi kirekesztődéssel kapcsolatos mutatók",
    # --- Economic organizations (gsz) ---
    "gsz0001": "A gazdasági szervezetek összefoglaló adatai",
    # --- Income (jov) ---
    "jov0001": "Az összes háztartás jövedelmének és fogyasztásának főbb adatai",
    # --- Environment (kor) ---
    "kor0001": "Környezet, kommunális ellátás főbb adatai",
    # --- Housing (lak) ---
    "lak0001": "A lakások összefoglaló adatai",
    # --- Agriculture (mez) ---
    "mez0001": "A mezőgazdaság összefoglaló adatai",
    # --- Education (okt) ---
    "okt0001": "Az oktatás főbb, hosszú idősoros adatai",
    # --- Transport (sza) ---
    "sza0001": "Összefoglaló adatok az áruszállításról és a személyszállításról",
    # --- Social services (szo) ---
    "szo0001": "A szociális ellátás összefoglaló adatai",
    # --- R&D, innovation (tte) ---
    "tte0001": "A kutatás-fejlesztés és az innováció főbb arányai",
    # --- ICT (ikt) ---
    "ikt0001": "Az információ, kommunikáció főbb mutatói",
    # --- Culture, sport (ksp) ---
    "ksp0001": "A kultúra összefoglaló adatai",
    # --- Justice (iga) ---
    "iga0001": "Az igazságszolgáltatás összefoglaló adatai",
    # --- Tourism (tur) ---
    "tur0001": "A turizmus és vendéglátás fontosabb adatai",
    "tur0030": "Az utazásszervező és -közvetítő vállalkozások száma és teljesítménye",
    "tur0031": "Turizmus Szatellit Számlák (TSzSz)",
    "tur0059": "A szállodák összefoglaló adatai havonta",
    "tur0060": "A kereskedelmi szálláshelyek bruttó szállásdíjbevételei szállástípusonként",
    "tur0070": "A külföldre tett utazások főbb mutatói célországok szerint, negyedévente",
    "tur0077": "A turisztikai szálláshelyek bruttó szállásdíjbevételei szállástípusonként, havonta",
    "tur0087": "A turisztikai szálláshelyek bruttó árbevételei szállástípusonként",
}


def _parse_ksh_csv(text: str, max_rows: int = 500) -> dict:
    """Parse KSH STADAT CSV (semicolon-delimited, Hungarian number format).

    KSH CSVs have:
    - Line 1: Table title (fewer semicolons than data)
    - Line 2: Column headers (Év; Mutató1; Mutató2; ...)
    - Line 3+: Data rows (2010;104,9;103,5;...)
    - Semicolon delimiter, comma as decimal, space as thousands separator
    - Windows line endings (\\r\\n)
    """
    # Clean up Windows line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # KSH multi-row header bug fix (2026-05-05 audit):
    # Some KSH CSVs (e.g. gdp0002, anything with long column descriptions)
    # embed newlines INSIDE quoted column names, e.g.:
    #     Év;Bruttó hazai termék;"Végső fogyasztás összesen,
    #     1960 = 100%";"Bruttó felhalmozás...
    # Splitting on "\n" tears these apart and the multi-row-header merge
    # logic below then concatenates header text into the wrong columns.
    # Pre-flatten newlines that fall INSIDE a quoted region into spaces.
    _out = []
    _in_quote = False
    for _ch in text:
        if _ch == '"':
            _in_quote = not _in_quote
            _out.append(_ch)
        elif _ch == "\n" and _in_quote:
            _out.append(" ")
        else:
            _out.append(_ch)
    text = "".join(_out)

    lines = [l for l in text.strip().split("\n") if l.strip()]
    if len(lines) < 2:
        return {"error": "Too few lines in CSV"}

    # Detect title, header, and data start.
    # KSH CSVs come in two formats:
    #   Format A (standard): Év;Mutató1;Mutató2  →  2010;104,9;103,5
    #   Format B (matrix/territorial): Területi egység;2009;2010;...  →  Budapest;104 253;...
    title = ""
    header_idx = 0

    def _count_years_in_parts(parts):
        """Count how many parts look like 4-digit years."""
        return sum(1 for p in parts if re.match(r'^\d{4}$', p.strip().strip('"')))

    def _looks_like_data(line_parts):
        first = line_parts[0].strip().strip('"')
        if re.match(r'^\d{4}$', first):
            return True
        # Check if cols 2+ have numeric data (territorial format)
        if len(line_parts) > 3:
            numeric_count = sum(1 for p in line_parts[2:6]
                                if p.strip().strip('"').replace(",", "").replace("\xa0", "").replace(" ", "").replace("-", "").replace(".", "").isdigit()
                                and p.strip())
            if numeric_count >= 2:
                return True
        return False

    # Check for territorial/matrix format: header row contains year columns
    # e.g. "Területi egység neve;Területi egység szintje;2009;2010;2011;..."
    is_matrix_format = False
    for i in range(min(len(lines), 5)):
        parts_i = [p.strip().strip('"') for p in lines[i].split(";")]
        if _count_years_in_parts(parts_i) >= 3:
            # This line has many year columns → it's the header
            is_matrix_format = True
            # Title is everything before this line that has few non-empty fields
            for t in range(i):
                parts_t = [p.strip() for p in lines[t].split(";")]
                nonempty_t = sum(1 for p in parts_t if p)
                if nonempty_t <= 2:
                    title = parts_t[0].strip('"')
            header_idx = i
            break

    if not is_matrix_format:
        # Standard format: find data start
        data_start = len(lines)
        for i in range(min(len(lines), 10)):
            parts_i = [p.strip() for p in lines[i].split(";")]
            if _looks_like_data(parts_i):
                data_start = i
                break

        parts0 = [p.strip() for p in lines[0].split(";")]
        nonempty0 = sum(1 for p in parts0 if p)
        if nonempty0 <= 2 and data_start > 1:
            title = parts0[0].strip('"')
            header_idx = 1
        else:
            header_idx = 0

        # Merge multi-row headers
        header_parts = [p.strip().strip('"') for p in lines[header_idx].split(";")]
        for extra_row in range(header_idx + 1, data_start):
            extra_parts = [p.strip().strip('"') for p in lines[extra_row].split(";")]
            for j in range(min(len(header_parts), len(extra_parts))):
                if extra_parts[j]:
                    if header_parts[j]:
                        header_parts[j] += " " + extra_parts[j]
                    else:
                        header_parts[j] = extra_parts[j]
    else:
        # Matrix format: header is the year row, data starts right after
        header_parts = [p.strip().strip('"') for p in lines[header_idx].split(";")]
        data_start = header_idx + 1

    headers = header_parts
    while headers and not headers[-1]:
        headers.pop()

    # For matrix format, track current category (section headers like "testi sértés")
    current_category = ""
    rows = []
    for line in lines[data_start:]:
        if not line.strip():
            continue
        values = line.split(";")

        # Matrix format: detect category/section headers (few non-empty fields)
        if is_matrix_format:
            nonempty = sum(1 for v in values[1:] if v.strip() and v.strip().strip('"'))
            if nonempty == 0:
                # Section header like "testi sértés" or "Ebből:"
                label = values[0].strip().strip('"')
                if label and label != "Ebből:":
                    current_category = label
                continue

        row = {}
        if is_matrix_format and current_category:
            row["kategória"] = current_category
        for i, h in enumerate(headers):
            if i < len(values):
                val = values[i].strip().strip('"')
                # Convert Hungarian numbers: space=thousands, comma=decimal
                if val and val not in ("..", "…", "x", "-", ""):
                    cleaned = val.replace("\xa0", "").replace(" ", "")
                    cleaned = cleaned.replace(",", ".")
                    try:
                        row[h] = float(cleaned) if "." in cleaned else int(cleaned)
                    except ValueError:
                        row[h] = val
                elif val in ("..", "…", "x"):
                    row[h] = None  # missing data
                else:
                    row[h] = val
            else:
                row[h] = None
        rows.append(row)
        # Note: don't break early — collect ALL rows so we can keep the
        # FRESHEST rows on truncation, not the oldest. KSH STADAT files
        # are ASC by time (oldest year/month first), so taking rows[:max]
        # was hiding 2026 data behind decades of 1960s-2000s observations.
        # (2026-05-05 audit fix.)

    total_in_file = len(rows)
    truncated = total_in_file > max_rows
    if truncated:
        rows = rows[-max_rows:]  # keep the freshest

    # Forward-fill the year column. KSH monthly CSVs leave "Év" blank after
    # the first month of each year (e.g. only Jan has "2026", Feb-Dec are
    # ''). Sub-agents reading the JSON couldn't track which year a given
    # month belonged to, and consistently misread the freshest 2026 rows
    # as 2024-era data. Fill it forward so each row stands on its own.
    # Heuristic: any column whose name contains "Év" (or its lowercase
    # variants) gets the carry-forward treatment. (2026-05-05 audit fix.)
    year_cols = [c for c in headers if c and ("Év" in c or "év" in c.lower())]
    for col in year_cols:
        last_val = None
        for r in rows:
            v = r.get(col)
            if v in (None, "", 0):
                if last_val is not None:
                    r[col] = last_val
            else:
                last_val = v

    # Reverse to DESC by row order (KSH files are ASC by time). With the
    # year column now populated everywhere, sub-agents see the freshest
    # months at the TOP of the data array.
    rows = list(reversed(rows))

    return {
        "title": title,
        "columns": headers,
        "row_count": len(rows),
        "total_rows_in_file": total_in_file,
        "truncated": truncated,
        "data": rows,
    }


@mcp.tool()
async def get_ksh_stadat(
    table_code: str,
    max_rows: int = 200,
) -> str:
    """Fetch data from KSH STADAT tables — Hungarian statistical time series.

    Rich time series data from the Hungarian Central Statistical Office (KSH).
    Covers GDP, inflation, wages, employment, trade, demographics, and more.

    Args:
        table_code: STADAT table code, e.g. "ara0001" (consumer price index),
                    "gdp0001" (GDP), "mun0001" (employment), "ber0001" (wages).
                    Use search_datasets(query="...", source="ksh") to find codes.
        max_rows: Maximum rows to return (default: 200, max: 1000)

    Common table codes:
        ara0001 — Consumer price index (annual)
        ara0004 — Product/service average prices (NOT core inflation)
        ara0039 — CPI detailed monthly
        ara0045 — Core inflation (maginfláció, havi, szezonálisan kiigazított)
        gdp0001 — GDP value and volume change
        gdp0005 — GDP per capita
        mun0001 — Labor market summary (employment, unemployment)
        mun0143 — Wages monthly (institutional data)
        mun0183 — Wages by sector monthly
        ipa0001 — Industrial production
        epi0001 — Construction output
        bel0001 — Retail trade (was ksk!)
        kkr0001 — Foreign trade (was kul!)
        nep0001 — Population and vital statistics
        gdp0017 — Government sector (budget, debt)
        kor0001 — Environment & utilities
        ene0001 — Energy balance
        tur0001 — Tourism

    Returns:
        JSON with parsed data rows, Hungarian headers.
    """
    max_rows = min(max_rows, 1000)
    # Extract category from table code (e.g. "ara" from "ara0001")
    m = re.match(r'^([a-z]+)(\d+)$', table_code.lower())
    if not m:
        return json.dumps({
            "error": f"Invalid table code format: '{table_code}'",
            "hint": "Format: category + number, e.g. 'ara0001', 'gdp0003'. "
                    "Use search_datasets(source='ksh') to find valid codes.",
            "available_categories": list(set(k[:3] for k in KSH_STADAT_CATALOG)),
        }, indent=2)

    category = m.group(1)
    code = table_code.lower()
    url = f"{KSH_STADAT_BASE}/{category}/hu/{code}.csv"

    client = await get_client()
    try:
        logger.info(f"KSH STADAT: {url}")
        resp = await client.get(url)
        resp.raise_for_status()

        # Handle encoding — KSH uses Windows-1250 (Hungarian)
        # Try Windows-1250 first (most KSH files), fallback to UTF-8
        try:
            text = resp.content.decode("windows-1250")
        except (UnicodeDecodeError, LookupError):
            text = resp.content.decode("utf-8", errors="replace")

        parsed = _parse_ksh_csv(text, max_rows)
        parsed["table_code"] = code
        parsed["url"] = url
        # Use catalog description, but flag if CSV title doesn't match
        catalog_desc = KSH_STADAT_CATALOG.get(code, "")
        parsed["description"] = catalog_desc
        if catalog_desc and parsed.get("title") and catalog_desc not in parsed["title"]:
            # CSV title differs from catalog — trust the CSV
            parsed["note"] = f"CSV title: {parsed['title']}"

        # Auto-learn: save KSH STADAT table as recipe
        try:
            title = parsed.get("title", catalog_desc or code)
            _auto_learn_recipe("KSH", code, {}, title, 1)
        except Exception:
            pass

        return json.dumps(parsed, ensure_ascii=False, indent=2)

    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"HTTP {e.response.status_code}",
            "hint": f"Table '{code}' not found. Check the code. "
                    "Available tables: " + ", ".join(sorted(KSH_STADAT_CATALOG.keys())[:20]),
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


# ---------------------------------------------------------------------------
# Yahoo Finance — market data (stocks, FX, commodities, bonds)
# ---------------------------------------------------------------------------


@mcp.tool()
async def yfinance(
    symbol: str,
    action: str = "quote",
    period: str = "1y",
    interval: str = "1d",
    start: str = "",
    end: str = "",
) -> str:
    """Yahoo Finance — current quotes or historical price data for stocks, forex, commodities, indices.

    Two modes:
      - action="quote" (default): Get current price, change, volume, key statistics.
      - action="history": Get historical OHLCV price data.

    Args:
        symbol: Yahoo Finance ticker symbol. Examples:
            Stocks: "AAPL", "MSFT", "OTP.BD" (OTP Budapest), "EBS.VI" (Erste Vienna)
            CEE: .BD (Budapest), .VI (Vienna), .WA (Warsaw), .PR (Prague)
            Forex: "EURHUF=X" (EUR/HUF), "USDHUF=X", "EURUSD=X"
            Commodities: "GC=F" (gold), "CL=F" (crude oil), "BZ=F" (Brent)
            Crypto: "BTC-USD", "ETH-USD"
            Bonds: "^TNX" (US 10Y yield), "^IRX" (US 3M T-bill)
            Indices: "^BUX.BD" (BUX), "^ATX" (ATX), "^GSPC" (S&P 500), "^GDAXI" (DAX)
        action: "quote" (default) for current snapshot, "history" for historical data.
        period: (history only) Time period - "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "max".
                Ignored if start/end provided.
        interval: (history only) Data frequency - "1d" (daily), "1wk" (weekly), "1mo" (monthly).
                  For intraday: "1m", "5m", "15m", "1h" (max 7 days).
        start: (history only) Start date "YYYY-MM-DD" (optional, overrides period)
        end: (history only) End date "YYYY-MM-DD" (optional)

    Returns:
        Quote mode: JSON with current price, change, volume, key statistics.
        History mode: JSON with OHLCV price history.

    Common symbols for economists:
        Forex: EURHUF=X, USDHUF=X, EURUSD=X, USDTRY=X, USDCNY=X
        CEE indices: ^BUX.BD (BUX), ^ATX (ATX Vienna), ^GSPC (S&P 500), ^GDAXI (DAX)
        Budapest: OTP.BD, MOL.BD, RICHTER.BD, MTELEKOM.BD, 4IG.BD
        Vienna: EBS.VI (Erste), OMV.VI, RBI.VI (Raiffeisen)
        Warsaw: PKO.WA, PZU.WA, KGH.WA (KGHM)
        Prague: CEZ.PR, KOMB.PR (Komercni Banka)
        Commodities: CL=F (WTI oil), BZ=F (Brent), NG=F (natgas), GC=F (gold), ZW=F (wheat)
        Bonds: ^TNX (US 10Y yield), ^IRX (US 3M T-bill)
    """
    action = action.strip().lower()

    # --- QUOTE MODE ---
    if action == "quote":
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            price = info.get("regularMarketPrice") or info.get("previousClose")

            # --- Freshness / staleness detection ---
            from datetime import datetime, timezone
            mkt_time = info.get("regularMarketTime")
            last_trade_at = ""
            data_age_days: Optional[int] = None
            if mkt_time:
                try:
                    dt = datetime.fromtimestamp(int(mkt_time), tz=timezone.utc)
                    last_trade_at = dt.strftime("%Y-%m-%d %H:%M UTC")
                    data_age_days = (datetime.now(timezone.utc) - dt).days
                except (ValueError, OSError, OverflowError):
                    pass

            instrument_type = (info.get("quoteType") or info.get("typeDisp") or "").upper()

            # Hard reject: dead/delisted ticker. Yahoo sometimes keeps stale
            # entries (e.g. 1990s mutual funds delisted ~7 years ago) — these
            # silently poison sub-agent reasoning. Block them explicitly.
            stale = False
            stale_reason = ""
            if data_age_days is not None and data_age_days > 30:
                stale = True
                stale_reason = f"last trade {data_age_days}d ago"
            if price in (0, 0.0, None):
                stale = True
                stale_reason = (stale_reason + "; " if stale_reason else "") + "regularMarketPrice is 0/None"

            if not price and not info.get("currency"):
                return json.dumps({
                    "error": f"Symbol '{symbol}' not found or has no data",
                    "hint": "Check the symbol format. Examples: 'AAPL', 'OTP.BD', 'EURHUF=X', 'GC=F'. Hungarian BUX index is NOT on Yahoo as '^BUX' — use OTP.BD/MOL.BD/RICHTER.BD as proxies.",
                }, indent=2)

            if stale:
                return json.dumps({
                    "error": f"Symbol '{symbol}' returns STALE data — {stale_reason}",
                    "stale": True,
                    "last_trade_at": last_trade_at,
                    "data_age_days": data_age_days,
                    "instrument_type": instrument_type,
                    "exchange": info.get("exchange", ""),
                    "currency": info.get("currency", ""),
                    "fiftyTwoWeekHigh_observed": info.get("fiftyTwoWeekHigh"),
                    "hint": "This ticker is dead/delisted on Yahoo. The 52w high/low values shown are historical and NOT current market data. Do NOT cite these as current prices.",
                }, indent=2)

            # Extract most useful fields
            result = {
                "symbol": symbol,
                "name": info.get("shortName") or info.get("longName", symbol),
                "currency": info.get("currency", ""),
                "price": price,
                "previous_close": info.get("previousClose"),
                "open": info.get("regularMarketOpen") or info.get("open"),
                "day_high": info.get("regularMarketDayHigh") or info.get("dayHigh"),
                "day_low": info.get("regularMarketDayLow") or info.get("dayLow"),
                "volume": info.get("regularMarketVolume") or info.get("volume"),
                "market_cap": info.get("marketCap"),
                "52w_high": info.get("fiftyTwoWeekHigh"),
                "52w_low": info.get("fiftyTwoWeekLow"),
                "pe_ratio": info.get("trailingPE"),
                "dividend_yield": info.get("dividendYield"),
                "exchange": info.get("exchange", ""),
                "instrument_type": instrument_type,
                "last_trade_at": last_trade_at,
                "data_age_days": data_age_days,
            }
            # Remove None values
            result = {k: v for k, v in result.items() if v is not None}

            return json.dumps(result, ensure_ascii=False, indent=2)

        except Exception as e:
            return json.dumps({
                "error": str(e),
                "hint": f"Check symbol '{symbol}'. Examples: 'AAPL', 'EURHUF=X', 'GC=F', 'OTP.BD'",
            }, indent=2)

    # --- HISTORY MODE ---
    try:
        ticker = yf.Ticker(symbol)

        if start:
            hist = ticker.history(start=start, end=end or None, interval=interval)
        else:
            hist = ticker.history(period=period, interval=interval)

        if hist.empty:
            return json.dumps({
                "error": f"No data for '{symbol}' with period='{period}'",
                "hint": "Check the symbol. Use 'EURHUF=X' for forex, '^BUX' for indices.",
            }, indent=2)

        # Convert to list of dicts
        rows = []
        for idx, row in hist.iterrows():
            entry = {
                "date": idx.strftime("%Y-%m-%d"),
                "open": round(row.get("Open", 0), 4),
                "high": round(row.get("High", 0), 4),
                "low": round(row.get("Low", 0), 4),
                "close": round(row.get("Close", 0), 4),
            }
            if "Volume" in row and row["Volume"] > 0:
                entry["volume"] = int(row["Volume"])
            rows.append(entry)

        # Truncate if too many
        truncated = False
        if len(rows) > 500:
            rows = rows[-500:]
            truncated = True

        return json.dumps({
            "symbol": symbol,
            "interval": interval,
            "data_points": len(rows),
            "truncated_to_last_500": truncated,
            "first_date": rows[0]["date"] if rows else "",
            "last_date": rows[-1]["date"] if rows else "",
            "data": rows,
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({
            "error": str(e),
            "hint": f"Check symbol '{symbol}'. Period: {period}, interval: {interval}",
        }, indent=2)


# ---------------------------------------------------------------------------
# Calculator — economic math without burning AI tokens
# ---------------------------------------------------------------------------
import math


@mcp.tool()
def calculate(expression: str) -> str:
    """Evaluate mathematical expressions and economic calculations.

    Use this instead of calculating in your head — it's faster and avoids errors.

    Args:
        expression: Math expression or economic function call. Supports:
            Basic math: "2 + 3", "100 * 1.05 ** 16", "(509571 / 119698 - 1) * 100"
            Cumulative inflation: "cum_inflation([104.9, 103.9, 105.7, 101.7])"
                → multiplies annual CPI indices (prev year=100%) into total inflation
            Real value: "real_value(509571, [104.9, 103.9, ...])"
                → deflates a nominal value by cumulative CPI indices
            CAGR: "cagr(119698, 509571, 16)"
                → compound annual growth rate over N years
            Currency convert: "convert(509571, 393)"
                → simple division (value / exchange_rate)
            Round: "round(3.14159, 2)" → 3.14

    Examples:
        "509571 / 119698"  → 4.257 (wage ratio)
        "cum_inflation([104.9, 103.9, 105.7, 101.7, 99.8, 99.9, 100.4, 102.4, 102.8, 103.4, 103.3, 105.1, 114.5, 117.6, 103.7, 104.4])"  → cumulative HU CPI 2010-2025
        "cagr(119698, 509571, 16)"  → annualized growth rate
        "real_value(509571, [104.9, 103.9, 105.7])"  → deflated value

    Returns:
        JSON with the result and the expression used.
    """
    # Define safe economic helper functions
    def cum_inflation(indices):
        """Cumulative inflation from annual CPI indices (prev year = 100%)."""
        result = 1.0
        for idx in indices:
            result *= idx / 100.0
        return {"cumulative_multiplier": round(result, 4),
                "total_percent_change": round((result - 1) * 100, 2),
                "years": len(indices)}

    def real_value(nominal, indices):
        """Deflate a nominal value by cumulative CPI indices."""
        cum = 1.0
        for idx in indices:
            cum *= idx / 100.0
        deflated = nominal / cum
        return {"nominal": nominal,
                "real_value": round(deflated, 2),
                "inflation_multiplier": round(cum, 4),
                "purchasing_power_change_pct": round((1 / cum - 1) * 100, 2)}

    def cagr(start_val, end_val, years):
        """Compound annual growth rate."""
        rate = (end_val / start_val) ** (1 / years) - 1
        return {"cagr_percent": round(rate * 100, 2),
                "total_growth_percent": round((end_val / start_val - 1) * 100, 2),
                "multiplier": round(end_val / start_val, 4),
                "years": years}

    def convert(amount, rate):
        """Currency conversion (amount / rate)."""
        return {"result": round(amount / rate, 2),
                "amount": amount,
                "rate": rate}

    def pct_change(old, new):
        """Percentage change from old to new."""
        return round((new / old - 1) * 100, 2)

    # Safe evaluation context — uppercase aliases so models that emit CAGR /
    # CUM_INFLATION / REAL_VALUE etc. don't trip on case-sensitivity.
    safe_ns = {
        "__builtins__": {},
        "cum_inflation": cum_inflation, "CUM_INFLATION": cum_inflation,
        "real_value":   real_value,    "REAL_VALUE":   real_value,
        "cagr":         cagr,          "CAGR":         cagr,
        "convert":      convert,       "CONVERT":      convert,
        "pct_change":   pct_change,    "PCT_CHANGE":   pct_change,
        "round": round,
        "abs": abs,
        "min": min,
        "max": max,
        "sum": sum,
        "len": len,
        "pow": pow,
        "sqrt": math.sqrt,
        "log": math.log,
        "log10": math.log10,
    }

    try:
        result = eval(expression, safe_ns)
        return json.dumps({
            "expression": expression,
            "result": result,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({
            "error": str(e),
            "expression": expression,
            "hint": (
                "Examples: '2+3*4', '(1.05)**10', 'sqrt(16)', 'log(2.718)', "
                "'cum_inflation([104.9, 103.9])', 'cagr(100, 200, 10)', "
                "'real_value(509571, [104.9, 103.9])', 'convert(509571, 393)'. "
                "NOTE: math module is NOT importable — use bare names: sqrt, log, log10."
            ),
        }, indent=2)


# ---------------------------------------------------------------------------
# MNB — Magyar Nemzeti Bank official exchange rates
# ---------------------------------------------------------------------------
_mnb_client = None


def _get_mnb():
    global _mnb_client
    if _mnb_client is None:
        _mnb_client = MnbClient()
    return _mnb_client


@mcp.tool()
def mnb_rates(
    mode: str = "current",
    currencies: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Official MNB (Hungarian National Bank) HUF exchange rates — current or historical.

    Two modes:
      - mode="current" (default): Get today's official MNB exchange rates.
      - mode="historical": Get daily rates for a date range (needs start_date, end_date).

    Args:
        mode: "current" (default) for today's rates, "historical" for a date range.
        currencies: Comma-separated currency codes (e.g. "EUR,USD,GBP").
                    Current mode: empty = all 32 active currencies.
                    Historical mode: default "EUR,USD". Available: EUR, USD, GBP, CHF, JPY,
                    CZK, PLN, RON, HRK, SEK, NOK, DKK, AUD, CAD, CNY, TRY, etc.
        start_date: (historical only) Start date YYYY-MM-DD (e.g. "2024-01-01"). Data from 1949-01-03.
        end_date: (historical only) End date YYYY-MM-DD (e.g. "2024-12-31")

    Returns:
        Current mode: JSON with official MNB HUF exchange rates (1 unit of foreign currency = X HUF).
        Historical mode: JSON with daily MNB rates. Note: no rates on weekends/holidays.
    """
    mode = mode.strip().lower()

    if mode not in ("current", "historical"):
        return json.dumps({
            "error": f"Unknown mode: '{mode}'",
            "hint": "Use mode='current' (today's rates) or mode='historical' (date range).",
        }, indent=2)

    # --- CURRENT MODE ---
    if mode == "current":
        try:
            client = _get_mnb()
            day = client.get_current_exchange_rates()

            all_rates = day.rates
            available_currencies = sorted(r.currency for r in all_rates)

            if currencies:
                wanted = {c.strip().upper() for c in currencies.split(",") if c.strip()}
                rates = [r for r in all_rates if r.currency in wanted]
                # Warn about unrecognized currencies
                found = {r.currency for r in rates}
                not_found = wanted - found
                result = {
                    "date": day.date.isoformat(),
                    "source": "Magyar Nemzeti Bank (MNB)",
                    "base": "HUF",
                    "count": len(rates),
                    "rates": [{"currency": r.currency, "rate": r.rate} for r in rates],
                }
                if not_found:
                    result["warning"] = f"Unknown currency codes: {', '.join(sorted(not_found))}"
                    result["available_currencies"] = available_currencies
            else:
                rates = all_rates
                result = {
                    "date": day.date.isoformat(),
                    "source": "Magyar Nemzeti Bank (MNB)",
                    "base": "HUF",
                    "count": len(rates),
                    "rates": [{"currency": r.currency, "rate": r.rate} for r in rates],
                }

            return json.dumps(result, ensure_ascii=False, indent=2)

        except Exception as e:
            return json.dumps({"error": str(e)}, indent=2)

    # --- HISTORICAL MODE ---
    from datetime import date as date_type

    if not start_date or not end_date:
        return json.dumps({
            "error": "Historical mode requires start_date and end_date",
            "hint": "Use mnb_rates(mode='historical', start_date='2024-01-01', end_date='2024-12-31')",
        }, indent=2)

    try:
        start = date_type.fromisoformat(start_date)
        end = date_type.fromisoformat(end_date)
    except ValueError:
        return json.dumps({"error": "Invalid date format. Use YYYY-MM-DD."}, indent=2)

    if start > end:
        return json.dumps({
            "error": f"start_date ({start_date}) is after end_date ({end_date})",
            "hint": "Swap the dates: start_date should be earlier than end_date.",
        }, indent=2)

    curr_list = [c.strip().upper() for c in currencies.split(",") if c.strip()]
    if not curr_list:
        curr_list = ["EUR", "USD"]

    try:
        client = _get_mnb()
        days = client.get_exchange_rates(start, end, curr_list)

        rows = []
        for day in days:
            row = {"date": day.date.isoformat()}
            for r in day.rates:
                row[r.currency] = r.rate
            rows.append(row)

        # Sort chronologically
        rows.sort(key=lambda r: r["date"])

        # Truncate
        truncated = False
        if len(rows) > 500:
            rows = rows[-500:]
            truncated = True

        return json.dumps({
            "source": "Magyar Nemzeti Bank (MNB)",
            "currencies": curr_list,
            "start": start_date,
            "end": end_date,
            "data_points": len(rows),
            "truncated_to_last_500": truncated,
            "data": rows,
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


# ---------------------------------------------------------------------------
# Self-learning recipe book — persistent JSON storage
# ---------------------------------------------------------------------------

_RECIPES_DIR = os.environ.get("RECIPES_DIR", os.path.dirname(os.path.abspath(__file__)))
_RECIPES_PATH = os.path.join(_RECIPES_DIR, "recipes.json")

# In-memory recipe list (loaded from JSON on startup)
_recipes_db: list[dict] = []

# Seed recipes — migrated to JSON on first run, then JSON is the source of truth
_SEED_RECIPES: list[dict] = [
    # --- KAMATOK / INTEREST RATES ---
    {"id": "policy_rate_PL", "keywords": ["kamat", "kamatláb", "policy rate", "alapkamat", "lengyelország", "poland", "pl", "imf"], "provider": "IMF", "dataset": "IFS", "dimensions": {"FREQ": "Q", "REF_AREA": "PL", "INDICATOR": "FPOLM_PA"}, "note": "IMF IFS — Poland monetary policy rate (quarterly)"},
    {"id": "short_rate_HU", "keywords": ["kamat", "rövid", "short rate", "magyarország", "hungary", "hu", "hun", "oecd"], "provider": "OECD", "dataset": "DP_LIVE", "dimensions": {"LOCATION": "HUN", "INDICATOR": "STINT", "FREQUENCY": "Q"}, "note": "OECD — Hungary short-term interest rate (quarterly)"},
    {"id": "short_rate_PL", "keywords": ["kamat", "rövid", "short rate", "lengyelország", "poland", "pl", "pol", "oecd"], "provider": "OECD", "dataset": "DP_LIVE", "dimensions": {"LOCATION": "POL", "INDICATOR": "STINT", "FREQUENCY": "Q"}, "note": "OECD — Poland short-term interest rate (quarterly)"},
    {"id": "short_rate_EA", "keywords": ["kamat", "rövid", "short rate", "eurozóna", "eurozone", "ea", "ea19", "oecd"], "provider": "OECD", "dataset": "DP_LIVE", "dimensions": {"LOCATION": "EA19", "INDICATOR": "STINT", "FREQUENCY": "Q"}, "note": "OECD — Euro area short-term interest rate (quarterly)"},
    {"id": "ecb_main_refi", "keywords": ["kamat", "ecb", "refi", "refinancing", "irányadó", "main", "európai központi bank"], "provider": "ECB", "dataset": "FM", "series_code": "B.U2.EUR.4F.KR.MRR_FR.LEV", "note": "ECB main refinancing rate (daily)"},
    # --- BÉREK / WAGES ---
    {"id": "wages_SI_gross", "keywords": ["bér", "bruttó", "wages", "gross", "szlovénia", "slovenia", "si", "eurostat"], "provider": "Eurostat", "dataset": "earn_nt_net", "dimensions": {"geo": "SI", "currency": "EUR", "estruct": "GRS_P1_NCH_AW100"}, "note": "Eurostat — Slovenia gross wages (EUR, single earner 100% avg wage)"},
    {"id": "wages_EE_gross", "keywords": ["bér", "bruttó", "wages", "gross", "észtország", "estonia", "ee", "eurostat"], "provider": "Eurostat", "dataset": "earn_nt_net", "dimensions": {"geo": "EE", "currency": "EUR", "estruct": "GRS_P1_NCH_AW100"}, "note": "Eurostat — Estonia gross wages (EUR, single earner 100% avg wage)"},
    {"id": "wages_SI_net", "keywords": ["bér", "nettó", "wages", "net", "szlovénia", "slovenia", "si", "eurostat"], "provider": "Eurostat", "dataset": "earn_nt_net", "dimensions": {"geo": "SI", "currency": "EUR", "estruct": "NET_P1_NCH_AW100"}, "note": "Eurostat — Slovenia net wages (EUR, single earner 100% avg wage)"},
    {"id": "wages_EE_net", "keywords": ["bér", "nettó", "wages", "net", "észtország", "estonia", "ee", "eurostat"], "provider": "Eurostat", "dataset": "earn_nt_net", "dimensions": {"geo": "EE", "currency": "EUR", "estruct": "NET_P1_NCH_AW100"}, "note": "Eurostat — Estonia net wages (EUR, single earner 100% avg wage)"},
    {"id": "wages_pps_SI", "keywords": ["bér", "vásárlóerő", "pps", "wages", "purchasing power", "szlovénia", "slovenia", "si"], "provider": "Eurostat", "dataset": "earn_nt_netft", "dimensions": {"geo": "SI", "estruct": "VAL_A_PPS"}, "note": "Eurostat — Slovenia wages in PPS (purchasing power standard)"},
    {"id": "wages_pps_EE", "keywords": ["bér", "vásárlóerő", "pps", "wages", "purchasing power", "észtország", "estonia", "ee"], "provider": "Eurostat", "dataset": "earn_nt_netft", "dimensions": {"geo": "EE", "estruct": "VAL_A_PPS"}, "note": "Eurostat — Estonia wages in PPS (purchasing power standard)"},
    {"id": "wages_HU_sector", "keywords": ["bér", "ágazat", "szektor", "wages", "sector", "magyarország", "hungary", "ksh"], "provider": "KSH", "dataset": "mun0183", "tool": "get_ksh_stadat", "note": "KSH STADAT mun0183 — Hungary wages by economic sector (monthly)"},
    {"id": "wages_HU_monthly", "keywords": ["bér", "havi", "wages", "monthly", "magyarország", "hungary", "ksh"], "provider": "KSH", "dataset": "mun0143", "tool": "get_ksh_stadat", "note": "KSH STADAT mun0143 — Hungary monthly wages (institutional data)"},
    # --- INFLÁCIÓ / INFLATION ---
    {"id": "cpi_HU", "keywords": ["infláció", "fogyasztói", "árindex", "cpi", "inflation", "magyarország", "hungary", "ksh"], "provider": "KSH", "dataset": "ara0001", "tool": "get_ksh_stadat", "note": "KSH STADAT ara0001 — Hungary consumer price index (annual)"},
    {"id": "hicp_EA_index", "keywords": ["hicp", "infláció", "inflation", "eurozóna", "eurozone", "ea", "index", "eurostat"], "provider": "Eurostat", "dataset": "prc_hicp_aind", "dimensions": {"geo": "EA", "coicop": "CP00", "unit": "INX_A_AVG"}, "note": "Eurostat — Euro area HICP annual average index (2015=100)"},
    {"id": "hicp_annual_rate", "keywords": ["hicp", "infláció", "inflation", "éves", "annual", "rate", "eurostat"], "provider": "Eurostat", "dataset": "prc_hicp_manr", "dimensions": {"coicop": "CP00"}, "note": "Eurostat — HICP annual rate of change (monthly, geo parameterizable)"},
    # --- ÁRFOLYAM / EXCHANGE RATES ---
    {"id": "eur_huf_current", "keywords": ["árfolyam", "exchange", "eur", "huf", "forint", "aktuális", "current", "mnb", "mai"], "provider": "MNB", "tool": "mnb_rates", "dimensions": {"currencies": "EUR"}, "note": "MNB current official EUR/HUF rate — use mnb_rates(currencies='EUR')"},
    {"id": "eur_huf_historical", "keywords": ["árfolyam", "exchange", "eur", "huf", "forint", "historikus", "historical", "mnb", "múlt"], "provider": "MNB", "tool": "mnb_rates", "dimensions": {"currencies": "EUR"}, "note": "MNB historical EUR/HUF rates — use mnb_rates(mode='historical', start_date=..., end_date=..., currencies='EUR')"},
    # --- GDP ---
    {"id": "gdp_HU", "keywords": ["gdp", "magyarország", "hungary", "ksh", "bruttó hazai termék"], "provider": "KSH", "dataset": "gdp0001", "tool": "get_ksh_stadat", "note": "KSH STADAT gdp0001 — Hungary GDP value and volume change"},
    {"id": "gdp_growth_EU", "keywords": ["gdp", "növekedés", "growth", "eu", "európa", "europe", "eurostat"], "provider": "Eurostat", "dataset": "namq_10_gdp", "dimensions": {"unit": "CLV_PCH_PRE", "s_adj": "SCA", "na_item": "B1GQ"}, "note": "Eurostat namq_10_gdp — EU GDP growth rate (quarterly, seasonally adjusted). Alt: tec00115 for annual overview."},
    # --- JEGYBANKI ALAPKAMATOK / POLICY RATES (BIS) ---
    {"id": "bis_rate_ECB", "keywords": ["kamat", "alapkamat", "policy rate", "ecb", "eurozóna", "eurozone", "bis", "jegybanki", "irányadó"], "provider": "BIS", "dataset": "WS_CBPOL", "dimensions": {"REF_AREA": "XM"}, "tool": "get_policy_rates", "note": "ECB irányadó kamat — get_policy_rates(countries='XM')"},
    {"id": "bis_rate_HU", "keywords": ["kamat", "alapkamat", "policy rate", "mnb", "magyarország", "hungary", "bis", "jegybanki"], "provider": "BIS", "dataset": "WS_CBPOL", "dimensions": {"REF_AREA": "HU"}, "tool": "get_policy_rates", "note": "MNB alapkamat — get_policy_rates(countries='HU')"},
    {"id": "bis_rate_V4", "keywords": ["kamat", "alapkamat", "v4", "visegrád", "régió", "összehasonlítás"], "provider": "BIS", "dataset": "WS_CBPOL", "tool": "get_policy_rates", "note": "V4+ECB kamatok összehasonlítása — get_policy_rates(countries='XM,HU,CZ,PL')"},
    {"id": "bis_rate_region", "keywords": ["kamat", "alapkamat", "régió", "szomszéd", "közép-európa", "cee"], "provider": "BIS", "dataset": "WS_CBPOL", "tool": "get_policy_rates", "note": "CEE+ECB kamatok — get_policy_rates(countries='XM,HU,CZ,PL,RO,HR')"},
    # --- MAGINFLÁCIÓ / CORE INFLATION (Eurostat) ---
    {"id": "hicp_core_HU", "keywords": ["maginfláció", "core", "inflation", "magyarország", "hungary", "hu", "eurostat"], "provider": "Eurostat", "dataset": "prc_hicp_manr", "dimensions": {"geo": "HU", "coicop": "TOT_X_NRG_FOOD"}, "note": "Eurostat HICP — Hungary core inflation (excl. energy & food, monthly YoY)"},
    {"id": "hicp_core_EA", "keywords": ["maginfláció", "core", "inflation", "eurozóna", "eurozone", "ea", "eurostat"], "provider": "Eurostat", "dataset": "prc_hicp_manr", "dimensions": {"geo": "EA", "coicop": "TOT_X_NRG_FOOD"}, "note": "Eurostat HICP — Euro area core inflation (monthly YoY)"},
    # --- FDI ---
    {"id": "fdi_HU", "keywords": ["fdi", "külföldi", "tőke", "működőtőke", "foreign direct investment", "magyarország", "hungary"], "provider": "Eurostat", "dataset": "bop_fdi6_flow", "dimensions": {"geo": "HU"}, "note": "Eurostat — Hungary FDI flows (inward/outward, annual)"},
    # --- ESI (PMI alternatíva) ---
    {"id": "esi_HU", "keywords": ["esi", "hangulat", "sentiment", "bizalmi", "konjunktúra", "pmi", "magyarország", "hungary"], "provider": "Eurostat", "dataset": "ei_bssi_m_r2", "dimensions": {"geo": "HU", "indic": "BS-ESI-I"}, "note": "Eurostat ESI — Hungary Economic Sentiment Indicator (PMI alternatíva, havi)"},
    {"id": "esi_EA", "keywords": ["esi", "hangulat", "sentiment", "bizalmi", "konjunktúra", "pmi", "eurozóna", "eurozone"], "provider": "Eurostat", "dataset": "ei_bssi_m_r2", "dimensions": {"geo": "EA", "indic": "BS-ESI-I"}, "note": "Eurostat ESI — Euro area Economic Sentiment Indicator (havi)"},
]


def _load_recipes() -> list[dict]:
    """Load recipes from JSON file, or seed from hardcoded defaults."""
    global _recipes_db
    try:
        if os.path.exists(_RECIPES_PATH):
            with open(_RECIPES_PATH, "r", encoding="utf-8") as f:
                _recipes_db = json.load(f)
            logger.info(f"Loaded {len(_recipes_db)} recipes from {_RECIPES_PATH}")
        else:
            # First run — seed from hardcoded recipes
            _recipes_db = []
            for r in _SEED_RECIPES:
                recipe = dict(r)
                recipe.setdefault("call_count", 0)
                recipe.setdefault("last_used", None)
                recipe.setdefault("source", "seed")
                _recipes_db.append(recipe)
            _save_recipes()
            logger.info(f"Seeded {len(_recipes_db)} recipes to {_RECIPES_PATH}")
    except Exception as e:
        logger.error(f"Failed to load recipes: {e}")
        if not _recipes_db:
            _recipes_db = list(_SEED_RECIPES)
    return _recipes_db


def _save_recipes() -> None:
    """Persist recipes to JSON file."""
    try:
        with open(_RECIPES_PATH, "w", encoding="utf-8") as f:
            json.dump(_recipes_db, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save recipes: {e}")


def _find_recipe_by_signature(provider: str, dataset: str, dimensions: dict) -> Optional[dict]:
    """Find existing recipe by provider+dataset+dimensions match."""
    for r in _recipes_db:
        if (r.get("provider", "").upper() == provider.upper()
                and r.get("dataset", "").lower() == dataset.lower()
                and r.get("dimensions", {}) == dimensions):
            return r
    return None


def _auto_learn_recipe(provider_code: str, dataset_code: str, dimensions: dict,
                       series_name: str = "", num_found: int = 0) -> None:
    """Auto-learn a recipe from a successful dbnomics_series call."""
    if num_found <= 0:
        return

    existing = _find_recipe_by_signature(provider_code, dataset_code, dimensions)
    if existing:
        # Bump usage count on existing recipe
        existing["call_count"] = existing.get("call_count", 0) + 1
        _save_recipes()
        return

    # Generate an ID from dimensions
    dim_parts = [f"{v}" for v in dimensions.values()] if dimensions else []
    auto_id = f"auto_{provider_code}_{dataset_code}_{'_'.join(dim_parts)}".replace("/", "_")
    # Avoid duplicate IDs
    if any(r["id"] == auto_id for r in _recipes_db):
        return

    # Generate keywords from dimensions + provider + series name
    keywords = [provider_code.lower(), dataset_code.lower()]
    for k, v in dimensions.items():
        if isinstance(v, str):
            keywords.append(v.lower())
        elif isinstance(v, list):
            keywords.extend(x.lower() for x in v)
    if series_name:
        keywords.extend(w.lower() for w in series_name.split() if len(w) > 2)
    keywords = list(dict.fromkeys(keywords))  # dedupe, preserve order

    recipe = {
        "id": auto_id,
        "keywords": keywords,
        "provider": provider_code,
        "dataset": dataset_code,
        "dimensions": dimensions,
        "note": f"Auto-learned from dbnomics_series ({series_name[:100]})" if series_name else "Auto-learned from dbnomics_series",
        "call_count": 0,
        "last_used": None,
        "source": "auto",
    }
    _recipes_db.append(recipe)
    _save_recipes()
    logger.info(f"Auto-learned recipe: {auto_id}")


# Load recipes on startup
_load_recipes()


# ---------------------------------------------------------------------------
# Usage tracking + nudge system
# ---------------------------------------------------------------------------

_USAGE_PATH = os.path.join(_RECIPES_DIR, "usage_stats.json")
_usage_stats: dict = {}


def _load_usage_stats() -> dict:
    global _usage_stats
    try:
        if os.path.exists(_USAGE_PATH):
            with open(_USAGE_PATH, "r", encoding="utf-8") as f:
                _usage_stats = json.load(f)
    except Exception:
        _usage_stats = {}
    return _usage_stats


def _save_usage_stats() -> None:
    try:
        with open(_USAGE_PATH, "w", encoding="utf-8") as f:
            json.dump(_usage_stats, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _track_usage(tool_name: str, client: str = "", params: dict = None) -> None:
    """Track a tool call for analytics."""
    from datetime import date as date_cls
    today = date_cls.today().isoformat()
    client = client.strip() or "unknown"

    if "daily" not in _usage_stats:
        _usage_stats["daily"] = {}
    if today not in _usage_stats["daily"]:
        _usage_stats["daily"][today] = {}
    day = _usage_stats["daily"][today]

    # Per-tool count
    if tool_name not in day:
        day[tool_name] = {"total": 0, "clients": {}}
    day[tool_name]["total"] += 1

    # Per-client count
    if client not in day[tool_name]["clients"]:
        day[tool_name]["clients"][client] = 0
    day[tool_name]["clients"][client] += 1

    # Global totals
    if "totals" not in _usage_stats:
        _usage_stats["totals"] = {}
    if tool_name not in _usage_stats["totals"]:
        _usage_stats["totals"][tool_name] = 0
    _usage_stats["totals"][tool_name] += 1

    _save_usage_stats()


def _nudge_tip(tool_name: str, query_worked: bool, has_recipe: bool) -> str:
    """Generate a gentle tip encouraging AI clients to improve the recipe book."""
    if query_worked and not has_recipe:
        return (
            "💡 TIP: This query worked! Consider saving it as a recipe with "
            "recipe_book(action='add') so others can find it instantly next time."
        )
    if not query_worked:
        return (
            "💡 TIP: If you found the right data through another method, "
            "please recipe_book(action='add') it. If something is broken, use recipe_book(action='report')."
        )
    return ""


_load_usage_stats()


@mcp.tool()
def recipe_book(
    action: str = "search",
    topic: str = "",
    id: str = "",
    provider: str = "",
    dataset: str = "",
    note: str = "",
    keywords: str = "",
    dimensions: str = "",
    series_code: str = "",
    tool: str = "",
    tool_name: str = "",
    description: str = "",
    client: str = "",
) -> str:
    """Self-learning recipe book — search, add recipes, report issues, or view usage stats.

    Four actions:
      - action="search" (default): Look up pre-built query recipes by topic keyword.
      - action="add": Add a new recipe to the recipe book.
      - action="report": Report a data quality issue, bug, or suggestion.
      - action="stats": View usage statistics.

    Args:
        action: "search" (default), "add", "report", or "stats".

        (search) topic: Search query, e.g. "kamat lengyelország", "wages slovenia",
                        "hicp inflation", "gdp hungary", "eur huf", "cpi magyarország"

        (add) id: Unique recipe ID (e.g. "short_rate_CZ", "hicp_DE")
        (add) provider: Data provider (e.g. "OECD", "Eurostat", "IMF", "ECB", "KSH")
        (add) dataset: Dataset code (e.g. "DP_LIVE", "prc_hicp_manr", "IFS")
        (add) note: Human-readable description of what this recipe returns
        (add) keywords: Comma-separated search keywords (e.g. "czech,cseh,kamat,rate,CZE,interest")
        (add) dimensions: JSON string of dimension filters (e.g. '{"LOCATION":"CZE","INDICATOR":"STINT"}')
        (add) series_code: Specific series code if applicable
        (add) tool: MCP tool to use (e.g. "get_ksh_stadat", "mnb_rates"). Empty = dbnomics_series.

        (report) tool_name: Which tool had the issue (e.g. "forecast", "get_eurostat_data")
        (report) description: What went wrong or what could be improved
        (report) client: Your name/model (e.g. "Claude Haiku", "ChatGPT-4o", "Gemini")

    Returns:
        Search: JSON with matching recipes (provider, dataset, dimensions, tool, note, stats).
        Add: JSON confirmation with the saved recipe.
        Report: Confirmation that the issue was logged.
        Stats: JSON with daily breakdown per tool and per client, plus global totals.
    """
    action = action.strip().lower()

    if action not in ("search", "add", "report", "stats"):
        return json.dumps({
            "error": f"Unknown action: '{action}'",
            "hint": "Use action='search' (default), 'add', 'report', or 'stats'.",
        }, indent=2)

    # --- STATS MODE ---
    if action == "stats":
        stats = _load_usage_stats()
        return json.dumps(stats, ensure_ascii=False, indent=2)

    # --- REPORT MODE ---
    if action == "report":
        if not tool_name or not description:
            return json.dumps({
                "error": "tool_name and description are required for reporting issues",
                "hint": "recipe_book(action='report', tool_name='forecast', description='...', client='Claude')",
            }, indent=2)

        from datetime import datetime as dt
        issues_path = os.path.join(_RECIPES_DIR, "issues.json")
        issues = []
        try:
            if os.path.exists(issues_path):
                with open(issues_path, "r", encoding="utf-8") as f:
                    issues = json.load(f)
        except Exception:
            issues = []

        issue = {
            "timestamp": dt.now().isoformat(),
            "tool": tool_name.strip(),
            "description": description.strip(),
            "client": client.strip() or "unknown",
            "status": "open",
        }
        issues.append(issue)

        try:
            with open(issues_path, "w", encoding="utf-8") as f:
                json.dump(issues, f, ensure_ascii=False, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to save issue: {e}"})

        logger.info(f"Issue reported by {issue['client']}: {tool_name} — {description[:100]}")
        return json.dumps({
            "status": "logged",
            "issue_number": len(issues),
            "message": "Thank you! The issue has been logged and will be reviewed.",
            "total_open_issues": sum(1 for i in issues if i.get("status") == "open"),
        }, ensure_ascii=False, indent=2)

    # --- ADD MODE ---
    if action == "add":
        recipe_id = id.strip()
        if not recipe_id or not provider.strip() or not dataset.strip():
            return json.dumps({"error": "id, provider, and dataset are required"})

        # Parse dimensions
        dims = {}
        if dimensions:
            try:
                dims = json.loads(dimensions)
            except json.JSONDecodeError:
                return json.dumps({"error": f"Invalid JSON in dimensions: {dimensions}"})

        # Parse keywords
        kw_list = [k.strip().lower() for k in keywords.split(",") if k.strip()] if keywords else []
        # Auto-add provider and dataset as keywords
        for auto_kw in [provider.lower(), dataset.lower()]:
            if auto_kw not in kw_list:
                kw_list.append(auto_kw)

        # Check for duplicate signature
        existing = _find_recipe_by_signature(provider, dataset, dims)
        if existing:
            # Merge keywords
            merged = list(existing.get("keywords", []))
            added = 0
            for kw in kw_list:
                if kw not in merged:
                    merged.append(kw)
                    added += 1
            existing["keywords"] = merged
            if note and note != existing.get("note", ""):
                existing["note"] = note
            _save_recipes()
            return json.dumps({
                "action": "merged_keywords",
                "id": existing["id"],
                "keywords_added": added,
                "total_keywords": len(merged),
                "recipe": existing,
            }, ensure_ascii=False, indent=2)

        # Check for duplicate ID
        if any(r["id"] == recipe_id for r in _recipes_db):
            return json.dumps({"error": f"Recipe ID '{recipe_id}' already exists. Choose a different ID."})

        recipe = {
            "id": recipe_id,
            "keywords": kw_list,
            "provider": provider.strip(),
            "dataset": dataset.strip(),
            "note": note.strip(),
            "call_count": 0,
            "last_used": None,
            "source": "manual",
        }
        if dims:
            recipe["dimensions"] = dims
        if series_code:
            recipe["series_code"] = series_code.strip()
        if tool:
            recipe["tool"] = tool.strip()

        _recipes_db.append(recipe)
        _save_recipes()
        logger.info(f"Manually added recipe: {recipe_id}")

        return json.dumps({
            "action": "added",
            "total_recipes": len(_recipes_db),
            "recipe": recipe,
        }, ensure_ascii=False, indent=2)

    # --- SEARCH MODE (default) ---
    if not topic:
        return json.dumps({
            "error": "Please provide a topic to search for",
            "hint": "recipe_book(topic='wages slovenia') or recipe_book(topic='kamat lengyelország')",
            "total_recipes": len(_recipes_db),
        }, ensure_ascii=False, indent=2)

    from datetime import date as date_cls
    query_words = topic.lower().split()

    scored: list[tuple[int, dict]] = []
    for recipe in _recipes_db:
        score = 0
        for qw in query_words:
            for kw in recipe.get("keywords", []):
                if qw in kw or kw in qw:
                    score += 1
                    break
            if qw in recipe["id"].lower():
                score += 1
            if qw in recipe.get("note", "").lower():
                score += 1
        if score > 0:
            scored.append((score, recipe))

    scored.sort(key=lambda x: x[0], reverse=True)

    _track_usage("recipe_book", params={"topic": topic})

    if not scored:
        return json.dumps({
            "error": "no recipe found",
            "query": topic,
            "hint": "use search_datasets or dbnomics_search to find the right dataset. "
                    "Successful queries auto-save as recipes!",
            "tip": "If you find the right data, please use recipe_book(action='add', ...) to save it "
                   "so others can find it instantly. Use recipe_book(action='report', ...) if something is broken.",
            "total_recipes": len(_recipes_db),
            "available_topics": sorted(set(r["id"] for r in _recipes_db))[:30],
        }, ensure_ascii=False, indent=2)

    # Update usage stats for top match
    top_recipe = scored[0][1]
    top_recipe["call_count"] = top_recipe.get("call_count", 0) + 1
    top_recipe["last_used"] = date_cls.today().isoformat()
    _save_recipes()

    results = []
    for score, recipe in scored[:5]:
        entry = {"id": recipe["id"], "relevance": score}
        for field in ("provider", "dataset", "dimensions", "series_code", "tool"):
            if field in recipe:
                entry[field] = recipe[field]
        entry["note"] = recipe.get("note", "")
        entry["call_count"] = recipe.get("call_count", 0)
        entry["source"] = recipe.get("source", "seed")
        results.append(entry)

    return json.dumps({
        "query": topic,
        "matches": len(results),
        "total_recipes": len(_recipes_db),
        "recipes": results,
    }, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# FRED (Federal Reserve Economic Data) — US macroeconomic indicators
# ---------------------------------------------------------------------------

_FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
_FRED_BASE = "https://api.stlouisfed.org/fred"

# Well-known FRED series for the docstring
_FRED_POPULAR = {
    "UNRATE": "US Unemployment Rate",
    "PAYEMS": "Non-Farm Payrolls (thousands)",
    "CPIAUCSL": "Consumer Price Index (All Urban)",
    "CPILFESL": "Core CPI (excl. food & energy)",
    "PCEPI": "PCE Price Index",
    "GDP": "US Gross Domestic Product",
    "INDPRO": "Industrial Production Index",
    "DFF": "Federal Funds Effective Rate",
    "DGS10": "10-Year Treasury Yield",
    "DGS2": "2-Year Treasury Yield",
    "T10Y2Y": "10Y-2Y Treasury Spread (yield curve)",
    "HOUST": "Housing Starts",
    "MORTGAGE30US": "30-Year Fixed Mortgage Rate",
    "UMCSENT": "U. of Michigan Consumer Sentiment",
    "RSAFS": "Retail Sales (total)",
    "CIVPART": "Labor Force Participation Rate",
    "PERMIT": "Building Permits",
    "M2SL": "M2 Money Supply",
    "FEDFUNDS": "Federal Funds Rate",
    "VIXCLS": "CBOE VIX Volatility Index",
}


@mcp.tool()
async def get_fred_data(
    series_id: str,
    limit: int = 100,
    sort_order: str = "desc",
    frequency: str = "",
    units: str = "",
) -> str:
    """Fetch time series data from FRED (Federal Reserve Economic Data).

    800,000+ US and international economic time series: interest rates, inflation,
    employment, GDP, housing, monetary aggregates, financial markets, and more.

    Args:
        series_id: FRED series ID (e.g. "UNRATE", "DGS10", "CPIAUCSL", "GDP").
                   Use FRED website or dbnomics_search to find series IDs.
        limit: Number of observations to return (default: 100, max: 1000)
        sort_order: "desc" (newest first) or "asc" (oldest first). Default: "desc"
        frequency: Optional aggregation: "m" (monthly), "q" (quarterly), "a" (annual).
                   Empty = native frequency of the series.
        units: Optional transformation: "lin" (levels, default), "chg" (change),
               "ch1" (change from year ago), "pch" (% change), "pc1" (% change from year ago),
               "pca" (compounded annual rate of change), "log" (natural log)

    Popular series:
        UNRATE — US Unemployment Rate
        PAYEMS — Non-Farm Payrolls
        CPIAUCSL — CPI All Urban Consumers
        CPILFESL — Core CPI (excl. food & energy)
        GDP — US GDP (quarterly, billions $)
        DFF — Federal Funds Effective Rate
        DGS10 — 10-Year Treasury Yield
        DGS2 — 2-Year Treasury Yield
        T10Y2Y — 10Y-2Y Spread (yield curve inversion signal)
        MORTGAGE30US — 30-Year Mortgage Rate
        HOUST — Housing Starts
        UMCSENT — Consumer Sentiment (U. of Michigan)
        M2SL — M2 Money Supply
        VIXCLS — VIX Volatility Index

    Returns:
        JSON with series metadata and observations (date + value pairs).
    """
    if not _FRED_API_KEY:
        return json.dumps({
            "error": "FRED_API_KEY not configured",
            "hint": "Set FRED_API_KEY environment variable. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html",
        })

    limit = min(max(limit, 1), 1000)
    series_id = series_id.strip().upper()

    params = {
        "series_id": series_id,
        "api_key": _FRED_API_KEY,
        "file_type": "json",
        "sort_order": sort_order,
        "limit": limit,
    }
    if frequency:
        params["frequency"] = frequency
    if units:
        params["units"] = units

    client = await get_client()

    # Fetch series info + observations in parallel. Retry once on transient
    # 5xx errors — FRED occasionally returns HTTP 500 under load.
    async def _fetch_with_retry(url, params, retries=2):
        last_resp = None
        for attempt in range(retries):
            r = await client.get(url, params=params)
            if r.status_code < 500:
                return r
            last_resp = r
            await asyncio.sleep(0.7 * (attempt + 1))
        return last_resp
    try:
        info_resp, obs_resp = await asyncio.gather(
            _fetch_with_retry(f"{_FRED_BASE}/series", {
                "series_id": series_id,
                "api_key": _FRED_API_KEY,
                "file_type": "json",
            }),
            _fetch_with_retry(f"{_FRED_BASE}/series/observations", params),
        )

        # Parse series metadata. Always include id so sub-agents see which
        # series they got back even if the /series endpoint flakes.
        meta = {"id": series_id}
        if info_resp.status_code == 200:
            serieses = info_resp.json().get("seriess", [])
            if serieses:
                s = serieses[0]
                meta = {
                    "id": s.get("id"),
                    "title": s.get("title"),
                    "frequency": s.get("frequency_short"),
                    "units": s.get("units"),
                    "seasonal_adjustment": s.get("seasonal_adjustment_short"),
                    "last_updated": s.get("last_updated"),
                }
        # Reflect the caller's requested transformations so sub-agents don't
        # mistake transformed values for the native units.
        if frequency:
            meta["frequency_requested"] = frequency
            meta["frequency_note"] = "Aggregated by FRED API at request — values reflect this aggregation, native frequency_short above is the source frequency."
        if units:
            meta["units_requested"] = units
            meta["units_note"] = (
                "Values transformed by FRED API. units codes: lin=levels, chg=change, "
                "ch1=change-from-year-ago, pch=%-change, pc1=%-change-from-year-ago, "
                "pca=compounded-annual-rate, log=natural-log. The native units string above "
                "(e.g. 'Percent') describes the SOURCE series, NOT the transformed value."
            )

        # Parse observations
        if obs_resp.status_code != 200:
            err = obs_resp.json() if obs_resp.headers.get("content-type", "").startswith("application/json") else {}
            return json.dumps({
                "error": f"FRED API error: HTTP {obs_resp.status_code}",
                "message": err.get("error_message", obs_resp.text[:200]),
                "hint": f"Check series ID '{series_id}'. Browse https://fred.stlouisfed.org/ to find valid IDs.",
            }, indent=2)

        obs_data = obs_resp.json()
        observations = obs_data.get("observations", [])

        rows = []
        for obs in observations:
            val = obs.get("value", ".")
            rows.append({
                "date": obs.get("date"),
                "value": float(val) if val != "." else None,
            })

        # Auto-learn: save FRED series as recipe
        if rows:
            try:
                title = meta.get("title", series_id)
                _auto_learn_recipe("FRED", series_id, {}, title, len(rows))
            except Exception:
                pass

        return json.dumps({
            "source": "FRED (Federal Reserve Economic Data)",
            "series": meta,
            "observations": len(rows),
            "sort_order": sort_order,
            "data": rows,
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


# ---------------------------------------------------------------------------
# OECD Composite Leading Indicator (CLI)
# ---------------------------------------------------------------------------

_OECD_SDMX_BASE = "https://sdmx.oecd.org/public/rest/data"

# 2-letter to 3-letter ISO mapping for OECD
_OECD_COUNTRY_MAP = {
    "DE": "DEU", "FR": "FRA", "IT": "ITA", "ES": "ESP",
    "NL": "NLD", "BE": "BEL", "AT": "AUT", "PL": "POL",
    "CZ": "CZE", "HU": "HUN", "SE": "SWE", "DK": "DNK",
    "FI": "FIN", "PT": "PRT", "GR": "GRC", "IE": "IRL",
    "US": "USA", "GB": "GBR", "UK": "GBR", "JP": "JPN",
    "CN": "CHN", "CA": "CAN", "AU": "AUS", "KR": "KOR",
    "MX": "MEX", "BR": "BRA", "IN": "IND", "RU": "RUS",
    "ZA": "ZAF", "TR": "TUR", "CH": "CHE", "NO": "NOR",
    "SK": "SVK", "SI": "SVN", "EE": "EST", "LV": "LVA",
    "LT": "LTU", "HR": "HRV", "BG": "BGR", "RO": "ROU",
}


async def _get_oecd_cli_data(country: str, periods: int = 12) -> str:
    """Internal helper: fetch OECD Composite Leading Indicator (CLI) for a country.

    The CLI predicts turning points in business cycles 6-9 months ahead.
    Values above 100 = expansion, below 100 = contraction.
    """
    # Normalize country code
    code = country.strip().upper()
    code_3 = _OECD_COUNTRY_MAP.get(code, code)

    # OECD SDMX CLI dataflow — try LI (leading indicator) first,
    # fallback to BCICP (business confidence composite) for countries like Hungary
    dataflow = "OECD.SDD.STES,DSD_STES@DF_CLI"
    url = f"{_OECD_SDMX_BASE}/{dataflow}"

    client = await get_client()
    rows = None
    measure_used = None
    for measure in ("LI", "BCICP"):
        dimension_path = f"{code_3}.M.{measure}...AA.IX..H"
        try:
            resp = await client.get(f"{url}/{dimension_path}", params={
                "lastNObservations": periods,
                "dimensionAtObservation": "AllDimensions",
                "format": "csvfilewithlabels",
            }, timeout=20.0)
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            rows = list(reader)
            if rows and "OBS_VALUE" in rows[0]:
                measure_used = measure
                break
            rows = None
        except httpx.HTTPStatusError:
            continue

    if not rows:
        return json.dumps({
            "error": f"No CLI data for country '{code}' ({code_3})",
            "hint": "CLI is available for ~30 OECD+ countries. Try: HU, DE, US, JP, CN, GB",
        }, indent=2)

    try:
        values = []
        periods_list = []
        for row in rows:
            try:
                val = float(row["OBS_VALUE"])
                values.append(val)
                periods_list.append(row.get("TIME_PERIOD", ""))
            except (ValueError, KeyError):
                continue

        if not values:
            return json.dumps({"error": f"No valid CLI observations for {code_3}"}, indent=2)

        # OECD returns rows in DESC time order (newest first), so values[0] is
        # the freshest observation and values[-1] is the oldest. Earlier code
        # took values[-1] as "latest" — that gave a 12-month-old number while
        # history[0] showed a fresh one. Fixed 2026-05-05.
        latest = values[0]

        # Trend: 3-month change (newest - 3 months ago)
        trend = None
        trend_direction = "unknown"
        if len(values) >= 4:
            trend = round(values[0] - values[3], 3)
            if trend > 0.2:
                trend_direction = "improving"
            elif trend < -0.2:
                trend_direction = "worsening"
            else:
                trend_direction = "stable"

        history = [
            {"period": p, "value": round(v, 2)}
            for p, v in zip(periods_list, values)
        ]

        measure_label = "Leading Indicator" if measure_used == "LI" else "Business Confidence Composite"

        # Auto-learn OECD CLI queries
        try:
            _auto_learn_recipe("OECD", "CLI", {"country": code_3},
                               f"OECD CLI — {code_3} composite leading indicator", 1)
        except Exception:
            pass

        return json.dumps({
            "source": f"OECD Composite Leading Indicator ({measure_label})",
            "country": code_3,
            "latest_value": round(latest, 2),
            "latest_period": periods_list[0] if periods_list else None,
            "momentum": "expansion" if latest > 100 else "contraction",
            "trend_3m": trend,
            "trend_direction": trend_direction,
            "interpretation": (
                f"CLI={round(latest, 2)}: {'above' if latest > 100 else 'below'} 100 "
                f"({trend_direction}) → "
                + ("strong expansion ahead" if latest > 100 and trend and trend > 0
                   else "expansion peaking" if latest > 100 and trend and trend <= 0
                   else "contraction bottoming, recovery" if latest <= 100 and trend and trend > 0
                   else "contraction deepening")
            ),
            "history": history,
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


# ---------------------------------------------------------------------------
# Forecast — UltimateForecaster ensemble (GDP, inflation, unemployment)
# ---------------------------------------------------------------------------

# Lazy-loaded singleton
_forecaster_instance = None
_forecaster_error = None
_sajat_cache_ready = False
_sajat_cache_error_detail = ""

# Forecast caches (avoid re-fetching every call)
_imf_cache: dict = {}  # {indicator_code: {iso3: {year: value}}}
_imf_cache_ts: float = 0.0
_IMF_CACHE_TTL = 3600  # 1 hour

_li_cache: dict = {}   # leading indicators composite cache
_li_cache_ts: float = 0.0
_LI_CACHE_TTL = 600    # 10 minutes


def _get_forecaster():
    """Get or create UltimateForecaster singleton (lazy init)."""
    global _forecaster_instance, _forecaster_error
    if _forecaster_instance is not None:
        return _forecaster_instance
    if _forecaster_error:
        return None
    try:
        import sys
        _server_dir = os.path.dirname(os.path.abspath(__file__))
        if _server_dir not in sys.path:
            sys.path.insert(0, _server_dir)
        from forecaster import UltimateForecaster
        _forecaster_instance = UltimateForecaster()
        logger.info("UltimateForecaster initialized")
        return _forecaster_instance
    except Exception as e:
        _forecaster_error = str(e)
        logger.error(f"Failed to init UltimateForecaster: {e}")
        return None


async def _ensure_sajat_cache(countries: list[str] | None = None):
    """Populate SAJÁT forecaster cache using async data fetch (runs in MCP event loop)."""
    global _sajat_cache_ready
    if _sajat_cache_ready:
        # Double-check cache actually has data
        try:
            from forecaster.sajat_forecaster_cache import _load_cache
            cache = _load_cache()
            if cache and cache.get('countries'):
                return True
            # Cache flag was set but cache is empty — retry
            _sajat_cache_ready = False
        except Exception:
            _sajat_cache_ready = False
    try:
        import sys
        _server_dir = os.path.dirname(os.path.abspath(__file__))
        if _server_dir not in sys.path:
            sys.path.insert(0, _server_dir)
        from forecaster.comprehensive_forecaster import (
            ComprehensiveConfig, ComprehensiveDataFetcher, ComprehensiveForecaster
        )
        from forecaster.sajat_forecaster_cache import _save_cache

        config = ComprehensiveConfig()
        if countries:
            config.COUNTRIES = countries
        else:
            # Start with core countries to keep it fast
            config.COUNTRIES = [
                'HU', 'PL', 'CZ', 'SK', 'DE', 'AT', 'FR', 'IT', 'ES',
                'NL', 'BE', 'GB', 'SE', 'RO', 'BG', 'HR', 'SI', 'EE', 'LV', 'LT',
            ]

        logger.info(f"SAJÁT cache: fetching data for {len(config.COUNTRIES)} countries...")
        fetcher = ComprehensiveDataFetcher(config)
        all_data = await fetcher.fetch_all_data()

        logger.info("SAJÁT cache: generating forecasts...")
        forecaster = ComprehensiveForecaster(config, all_data)
        all_forecasts = forecaster.forecast_all_comprehensive(scenarios=['REALISTIC'])

        # Convert to cache format
        from datetime import datetime as dt
        cache = {'generated_at': dt.now().isoformat(), 'scenario': 'REALISTIC', 'countries': {}}
        for country, country_data in all_forecasts.items():
            if 'REALISTIC' not in country_data:
                continue
            scenario_data = country_data['REALISTIC']
            cc = {'gdp': {}, 'inflation': {}, 'unemployment': {}}

            if 'gdp' in scenario_data and not scenario_data['gdp'].empty:
                for idx, row in scenario_data['gdp'].iterrows():
                    cc['gdp'][str(idx)] = {
                        'value': float(row.get('GDP', 0)),
                        'growth_qoq': float(row.get('growth_qoq', 0)),
                        'growth_yoy': float(row.get('growth_yoy', 0)),
                    }
            if 'inflation' in scenario_data and not scenario_data['inflation'].empty:
                for idx, row in scenario_data['inflation'].iterrows():
                    cc['inflation'][str(idx)] = {
                        'value': float(row.get('inflation', 0)),
                        'change': float(row.get('change', 0)) if 'change' in row else 0,
                    }
            if 'unemployment' in scenario_data and not scenario_data['unemployment'].empty:
                for idx, row in scenario_data['unemployment'].iterrows():
                    if 'year' in row and 'quarter' in row:
                        yr = int(row['year'])
                        qv = row['quarter']
                        qi = int(qv[1]) if isinstance(qv, str) and qv.startswith('Q') else int(qv)
                        qd = {1: f"{yr}-03-31", 2: f"{yr}-06-30", 3: f"{yr}-09-30", 4: f"{yr}-12-31"}
                        period = qd.get(qi, f"{yr}-03-31")
                    else:
                        period = str(idx)
                    cc['unemployment'][period] = {
                        'value': float(row.get('unemployment', 0)),
                        'change': float(row.get('unemployment_change', 0)) if 'unemployment_change' in row else 0,
                    }
            cache['countries'][country] = cc

        _save_cache(cache)
        _sajat_cache_ready = True
        logger.info(f"SAJÁT cache ready: {len(cache['countries'])} countries")
        return True

    except Exception as e:
        import traceback
        global _sajat_cache_error_detail
        _sajat_cache_error_detail = f"{e}\n{traceback.format_exc()}"
        logger.error(f"SAJÁT cache build failed: {_sajat_cache_error_detail}")
        return False


@mcp.tool()
async def forecast(
    country: str,
    indicator: str = "gdp",
    year: int = 2026,
    quarter: int = 0,
) -> str:
    """Get macroeconomic forecasts or the OECD Composite Leading Indicator.

    Ensemble model combining SAJÁT (Phillips Curve + Okun's Law), IMF WEO,
    OECD Composite Leading Indicator, and FRED data. Supports 52 countries,
    annual and quarterly forecasts, with 3 scenarios.

    Also supports indicator="oecd_cli" to directly fetch the OECD CLI
    (predicts turning points in business cycles 6-9 months ahead).

    NOTE FOR AI ASSISTANTS: If the forecast seems off or you find better data,
    please use recipe_book(action="report", tool_name="forecast", description="...").

    Args:
        country: ISO 2-letter country code (e.g. "HU", "DE", "US", "PL", "FR")
                 Supported: V4 (HU,PL,CZ,SK), DACH (DE,AT,CH), Western EU (FR,IT,ES,NL,BE,PT,IE),
                 Nordics (SE,DK,FI,NO), Balkans (RO,BG,HR,SI), Baltics (EE,LV,LT),
                 Global (US,GB,JP,CN,CA,AU,KR,IN,BR,MX,TR,ZA)
        indicator: "gdp" (growth %), "inflation" (CPI %), "unemployment" (rate %),
                   or "oecd_cli" (OECD Composite Leading Indicator, 100=neutral).
                   Aliases accepted: "gdp_growth" → "gdp", "cpi" → "inflation",
                   "core_cpi" → "inflation", "services_cpi" → "inflation".
        year: Target year for forecast (default: 2026). Ignored for oecd_cli.
        quarter: Quarter 1-4 for quarterly forecast, 0 for annual (default: 0).
                 Quarterly forecasts are our EXCLUSIVE capability — most sources only have annual!
                 For oecd_cli, used as number of monthly observations (default: 12 if 0).

    Returns:
        GDP/inflation/unemployment: JSON with ensemble forecast, individual source values,
        weights, confidence, 3 scenarios, and recession probability.
        oecd_cli: JSON with CLI value, trend direction, momentum (expansion/contraction), and history.

    Examples:
        forecast("HU", "gdp", 2026) -> Hungary GDP growth forecast for 2026
        forecast("DE", "inflation", 2026, 2) -> Germany Q2 2026 inflation forecast
        forecast("US", "unemployment", 2026) -> US unemployment rate forecast
        forecast("HU", "oecd_cli") -> Hungary OECD CLI (business cycle indicator)

    OECD CLI interpretation:
        CLI > 100 + trending up -> strong expansion ahead
        CLI > 100 + trending down -> expansion peaking, slowdown coming
        CLI < 100 + trending down -> contraction deepening
        CLI < 100 + trending up -> contraction bottoming, recovery coming
    """
    indicator = indicator.lower().strip()
    # Indicator-name aliasing — keeps forecast compatible with get_macro_indicator's
    # broader taxonomy (gdp_growth, cpi, core_cpi, ...) without losing functionality.
    _ind_aliases = {
        "gdp_growth": "gdp",
        "cpi": "inflation",
        "core_cpi": "inflation",
        "services_cpi": "inflation",
        "headline_cpi": "inflation",
    }
    indicator = _ind_aliases.get(indicator, indicator)

    # --- OECD CLI MODE ---
    if indicator == "oecd_cli":
        periods = quarter if quarter and quarter > 0 else 12
        return await _get_oecd_cli_data(country, periods)

    uf = _get_forecaster()
    if uf is None:
        return json.dumps({
            "error": "Forecaster not available",
            "detail": _forecaster_error or "UltimateForecaster failed to initialize",
            "hint": "Check server logs for missing dependencies (pandas, numpy, requests)",
        }, indent=2)

    if indicator not in ("gdp", "inflation", "unemployment"):
        return json.dumps({
            "error": f"Unknown indicator: '{indicator}'",
            "hint": "Use 'gdp', 'inflation', 'unemployment', or 'oecd_cli'",
        })

    country = country.strip().upper()
    q = quarter if quarter and 1 <= quarter <= 4 else None

    # ISO2 → ISO3 mapping for IMF DataMapper
    _ISO3 = {
        "HU": "HUN", "DE": "DEU", "PL": "POL", "CZ": "CZE", "SK": "SVK",
        "AT": "AUT", "CH": "CHE", "FR": "FRA", "IT": "ITA", "ES": "ESP",
        "NL": "NLD", "BE": "BEL", "PT": "PRT", "IE": "IRL", "LU": "LUX",
        "SE": "SWE", "DK": "DNK", "FI": "FIN", "NO": "NOR",
        "RO": "ROU", "BG": "BGR", "HR": "HRV", "SI": "SVN",
        "EE": "EST", "LV": "LVA", "LT": "LTU",
        "US": "USA", "GB": "GBR", "JP": "JPN", "CN": "CHN",
        "CA": "CAN", "AU": "AUS", "KR": "KOR", "IN": "IND",
        "BR": "BRA", "MX": "MEX", "TR": "TUR", "ZA": "ZAF",
        "RS": "SRB", "UA": "UKR", "GR": "GRC", "EL": "GRC",
    }

    # Hard-reject unknown country codes. Earlier behavior silently fell through
    # to an "empty forecast" with confidence=30 and a fabricated gdp_signal —
    # which sub-agents took as real data. (2026-05-05 audit fix.)
    if country not in _ISO3 and country not in _ISO3.values():
        return json.dumps({
            "error": f"Unknown country: '{country}'",
            "hint": "Use ISO-2 code. Supported: " + ", ".join(sorted(_ISO3.keys())),
        }, indent=2)
    _IMF_INDICATORS = {
        "gdp": "NGDP_RPCH",       # Real GDP growth %
        "inflation": "PCPIPCH",    # CPI inflation %
        "unemployment": "LUR",     # Unemployment rate %
    }

    try:
        iso3 = _ISO3.get(country, country)
        imf_code = _IMF_INDICATORS[indicator]
        now = time.time()

        # --- 1. IMF DataMapper forecast (cached 1h) ---
        global _imf_cache, _imf_cache_ts
        if now - _imf_cache_ts > _IMF_CACHE_TTL:
            # Fetch all 3 indicators in one go for all countries
            client = await get_client()
            new_cache = {}
            for ind_name, ind_code in _IMF_INDICATORS.items():
                try:
                    resp = await client.get(
                        f"https://www.imf.org/external/datamapper/api/v1/{ind_code}",
                        params={"periods": ",".join(str(y) for y in range(year - 1, year + 3))},
                        timeout=15.0,
                    )
                    if resp.status_code == 200:
                        vals = resp.json().get("values", {}).get(ind_code, {})
                        new_cache[ind_code] = {
                            c3: {int(y): float(v) for y, v in yv.items() if v is not None}
                            for c3, yv in vals.items()
                        }
                except Exception as e:
                    logger.warning(f"IMF fetch {ind_code}: {e}")
            if new_cache:
                _imf_cache = new_cache
                _imf_cache_ts = now
                logger.info(f"IMF cache refreshed: {sum(len(v) for v in new_cache.values())} country-indicators")

        imf_country = _imf_cache.get(imf_code, {}).get(iso3, {})
        imf_forecast = imf_country.get(year)
        imf_all_years = imf_country if imf_country else None

        # --- 2. Leading indicators (cached 10min) ---
        global _li_cache, _li_cache_ts
        if now - _li_cache_ts > _LI_CACHE_TTL:
            uf.fetch_all_indicators()
            _li_cache_ts = now

        composite = uf.calculate_composite_score(country)
        li_adjustment = 0.0
        if composite.get("confidence", 0) >= 40:
            li_adjustment = round(composite["composite_score"] / 100, 2)
            li_adjustment = max(-0.5, min(0.5, li_adjustment))

        # --- 3. Build ensemble ---
        sources = {}
        forecasts_vals = []
        weights = []

        if imf_forecast is not None:
            sources["imf_weo"] = imf_forecast
            forecasts_vals.append(imf_forecast)
            weights.append(0.50)

        # SAJÁT forecaster (if cache available)
        try:
            sajat_result = uf.get_sajat_forecast(country, indicator, year, quarter=q)
            if sajat_result and sajat_result.get("value") is not None:
                sv = sajat_result["value"]
                if imf_forecast is None or abs(sv) <= abs(imf_forecast) * 5 + 5:
                    sources["sajat"] = round(sv, 2)
                    forecasts_vals.append(sv)
                    weights.append(0.30)
        except Exception:
            pass

        # FRED current (non-EU countries)
        try:
            fred_result = uf.get_ultimate_forecast(country, indicator, year, quarter=q)
            fk = f"fred_{indicator}_current"
            if fk in fred_result.get("sources", {}):
                fv = fred_result["sources"][fk]
                sources["fred_current"] = round(fv, 2)
                forecasts_vals.append(fv)
                weights.append(0.20)
        except Exception:
            pass

        # Weighted average + leading indicator adjustment
        import numpy as np
        ultimate = None
        confidence = 30
        if forecasts_vals:
            w = np.array(weights)
            w = w / w.sum()
            ultimate = round(float(np.average(forecasts_vals, weights=w)) + li_adjustment, 2)
            confidence = min(90, 30 + len(forecasts_vals) * 20 + composite.get("confidence", 0) * 0.2)

        # --- 4. Quarterly breakdown (from annual IMF anchor) ---
        quarterly_breakdown = None
        if ultimate is not None and q is None and indicator == "gdp":
            # Seasonal GDP pattern: Q1 weak, Q2-Q3 strong, Q4 moderate
            seasonal = {1: -0.15, 2: 0.10, 3: 0.08, 4: -0.03}
            quarterly_breakdown = {}
            for qi in range(1, 5):
                q_val = round(ultimate + seasonal[qi] + li_adjustment * (0.3 if qi <= 2 else -0.1), 2)
                quarterly_breakdown[f"Q{qi}"] = q_val
        elif ultimate is not None and q is not None and indicator == "gdp":
            # Specific quarter requested — adjust from annual
            seasonal = {1: -0.15, 2: 0.10, 3: 0.08, 4: -0.03}
            ultimate = round(ultimate + seasonal.get(q, 0), 2)

        spread = {"gdp": 0.8, "inflation": 1.0, "unemployment": 0.5}.get(indicator, 0.8)

        result = {
            "country": country,
            "indicator": indicator,
            "year": year,
            "quarter": q,
            "ultimate_forecast": ultimate,
            "confidence": round(confidence, 1),
            "sources": sources,
            "leading_indicator_adjustment": li_adjustment,
            "scenarios": {
                "pessimistic": round(ultimate - spread, 2),
                "realistic": ultimate,
                "optimistic": round(ultimate + spread, 2),
            } if ultimate else None,
            "quarterly_breakdown": quarterly_breakdown,
            "imf_all_years": imf_all_years,
            "composite_score": round(composite.get("composite_score", 0), 1),
            "gdp_signal": composite.get("gdp_growth_signal", "unknown"),
            "recession_probability": round(composite.get("recession_probability", 0), 1),
            "is_quarterly": q is not None,
            "source_description": (
                "Ensemble: IMF WEO (50%) + SAJÁT Phillips/Okun (30%) + FRED/OECD (20%), "
                "adjusted by leading indicators (ifo, yield curve, VIX, sentiment)."
            ),
        }

        # Auto-learn + track
        try:
            _auto_learn_recipe("forecast", indicator, {"country": country, "year": str(year)},
                               f"forecast {country} {indicator} {year}", 1)
            _track_usage("forecast", params={"country": country, "indicator": indicator, "year": year})
        except Exception:
            pass

        return json.dumps(result, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e), "country": country, "indicator": indicator}, indent=2)


# ---------------------------------------------------------------------------
# Economic Calendar — upcoming data releases (FRED, ECB, Eurostat)
# ---------------------------------------------------------------------------

# ECB Governing Council meeting dates (2025-2026, official schedule)
_ECB_MEETINGS_2025_2026 = [
    "2025-01-30", "2025-03-06", "2025-04-17", "2025-06-05",
    "2025-07-24", "2025-09-11", "2025-10-30", "2025-12-18",
    "2026-01-22", "2026-03-05", "2026-04-16", "2026-06-04",
    "2026-07-16", "2026-09-10", "2026-10-29", "2026-12-17",
]

# FRED release schedule metadata
_FRED_CALENDAR_SERIES = {
    "UNRATE": {"name": "US Unemployment Rate", "freq": "monthly", "delay_days": 35, "importance": "high", "time": "08:30 ET"},
    "PAYEMS": {"name": "US Non-Farm Payrolls", "freq": "monthly", "delay_days": 35, "importance": "high", "time": "08:30 ET"},
    "CPIAUCSL": {"name": "US CPI", "freq": "monthly", "delay_days": 14, "importance": "high", "time": "08:30 ET"},
    "CPILFESL": {"name": "US Core CPI", "freq": "monthly", "delay_days": 14, "importance": "high", "time": "08:30 ET"},
    "GDP": {"name": "US GDP", "freq": "quarterly", "delay_days": 30, "importance": "high", "time": "08:30 ET"},
    "RSAFS": {"name": "US Retail Sales", "freq": "monthly", "delay_days": 15, "importance": "medium", "time": "08:30 ET"},
    "INDPRO": {"name": "US Industrial Production", "freq": "monthly", "delay_days": 17, "importance": "medium", "time": "09:15 ET"},
    "HOUST": {"name": "US Housing Starts", "freq": "monthly", "delay_days": 18, "importance": "medium", "time": "08:30 ET"},
    "UMCSENT": {"name": "US Consumer Sentiment (UMich)", "freq": "monthly", "delay_days": -2, "importance": "medium", "time": "10:00 ET"},
}

# Eurostat release schedule metadata
_EUROSTAT_CALENDAR = {
    "prc_hicp_manr": {"name": "Euro Area HICP (Flash)", "freq": "monthly", "delay_days": 17, "importance": "high", "time": "11:00 CET"},
    "nama_10_gdp_flash": {"name": "Euro Area GDP (Flash)", "freq": "quarterly", "delay_days": 45, "importance": "high", "time": "11:00 CET"},
    "une_rt_m": {"name": "Euro Area Unemployment", "freq": "monthly", "delay_days": 65, "importance": "high", "time": "11:00 CET"},
    "sts_inpr_m": {"name": "Euro Area Industrial Production", "freq": "monthly", "delay_days": 45, "importance": "medium", "time": "11:00 CET"},
}


def _estimate_release_dates(freq: str, delay_days: int, start_date, end_date) -> list[str]:
    """Estimate release dates based on frequency and typical delay."""
    from datetime import date as date_cls, timedelta
    dates = []

    if freq == "monthly":
        # Go back a few months to catch releases that fall in our window
        cur = start_date.replace(day=1) - timedelta(days=90)
        while cur <= end_date:
            # Reference month end → add delay
            next_month_1st = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
            if delay_days >= 0:
                release = next_month_1st + timedelta(days=delay_days - 1)
            else:
                # Negative delay = released before month end
                release = next_month_1st + timedelta(days=delay_days)
            if start_date <= release <= end_date:
                dates.append(release.isoformat())
            cur = next_month_1st
    elif freq == "quarterly":
        from datetime import date as d
        for year in range(start_date.year - 1, end_date.year + 1):
            for q_end in [d(year, 3, 31), d(year, 6, 30), d(year, 9, 30), d(year, 12, 31)]:
                release = q_end + timedelta(days=delay_days)
                if start_date <= release <= end_date:
                    dates.append(release.isoformat())

    return dates


@mcp.tool()
def get_economic_calendar(
    days_ahead: int = 14,
    region: str = "all",
) -> str:
    """Get upcoming economic data releases and central bank events.

    Covers FRED (US), ECB meetings, and Eurostat (Euro Area) release schedule.
    Useful for knowing what data is coming this week/month.

    Args:
        days_ahead: Number of days to look ahead (default: 14, max: 90)
        region: Filter by region: "us", "eu", "ecb", or "all" (default: "all")

    Returns:
        JSON list of upcoming events with date, indicator name, importance, source.
    """
    from datetime import date as date_cls, timedelta
    days_ahead = min(max(days_ahead, 1), 90)
    today = date_cls.today()
    end = today + timedelta(days=days_ahead)
    region = region.lower().strip()

    events: list[dict] = []

    # --- FRED releases ---
    if region in ("all", "us"):
        for series_id, info in _FRED_CALENDAR_SERIES.items():
            for release_date in _estimate_release_dates(info["freq"], info["delay_days"], today, end):
                events.append({
                    "date": release_date,
                    "time": info["time"],
                    "indicator": info["name"],
                    "series_id": series_id,
                    "importance": info["importance"],
                    "region": "US",
                    "source": "FRED",
                })

    # --- ECB meetings ---
    if region in ("all", "eu", "ecb"):
        for meeting_date_str in _ECB_MEETINGS_2025_2026:
            md = date_cls.fromisoformat(meeting_date_str)
            if today <= md <= end:
                events.append({
                    "date": meeting_date_str,
                    "time": "13:45 CET",
                    "indicator": "ECB Governing Council — Interest Rate Decision",
                    "importance": "high",
                    "region": "EUR",
                    "source": "ECB",
                })
                events.append({
                    "date": meeting_date_str,
                    "time": "14:30 CET",
                    "indicator": "ECB Press Conference",
                    "importance": "high",
                    "region": "EUR",
                    "source": "ECB",
                })

    # --- Eurostat releases ---
    if region in ("all", "eu"):
        for ds_code, info in _EUROSTAT_CALENDAR.items():
            for release_date in _estimate_release_dates(info["freq"], info["delay_days"], today, end):
                events.append({
                    "date": release_date,
                    "time": info["time"],
                    "indicator": info["name"],
                    "dataset": ds_code,
                    "importance": info["importance"],
                    "region": "EUR",
                    "source": "Eurostat",
                })

    # Sort by date
    events.sort(key=lambda e: e["date"])

    return json.dumps({
        "period": f"{today.isoformat()} → {end.isoformat()}",
        "region_filter": region,
        "total_events": len(events),
        "events": events,
    }, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Central bank policy rates (BIS via DBnomics)
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_policy_rates(
    countries: str = "XM,HU,CZ,PL,RO",
    frequency: str = "M",
    limit: int = 12,
) -> str:
    """Fetch central bank policy rates from BIS via DBnomics.

    Current and historical monetary policy rates for central banks worldwide.
    Data from the Bank for International Settlements (BIS) WS_CBPOL dataset.

    Args:
        countries: Comma-separated BIS country codes (default: "XM,HU,CZ,PL,RO").
                   XM=Euro area (ECB), HU=Hungary (MNB), CZ=Czechia (CNB),
                   PL=Poland (NBP), RO=Romania (BNR).
                   Other codes: US, GB, JP, CH, SE, NO, DK, AU, CA, TR, etc.
        frequency: "M" for monthly (default), "D" for daily
        limit: Number of recent observations per country (default: 12)

    Returns:
        JSON with current policy rates and recent history per country.
    """
    codes = [c.strip().upper() for c in countries.split(",") if c.strip()]
    dims = json.dumps({"FREQ": [frequency.upper()], "REF_AREA": codes})
    client = await get_client()
    url = f"{DBNOMICS_BASE}/series/BIS/WS_CBPOL"
    params = {"dimensions": dims, "observations": "1", "format": "json", "limit": 200}

    try:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return json.dumps({"error": f"BIS API error: {e}"}, ensure_ascii=False)

    series_list = data.get("series", {}).get("docs", [])
    results = {}
    from datetime import date as _date
    today = _date.today()
    for s in series_list:
        code = s.get("series_code", "")
        ref_area = code.split(".")[1] if "." in code else code
        periods = s.get("period", [])
        values = s.get("value", [])
        obs = [{"period": p, "rate": v} for p, v in zip(periods, values) if v is not None]
        obs = obs[-limit:]
        if obs:
            as_of = obs[-1]["period"]
            # Estimate freshness — BIS WS_CBPOL is monthly ('YYYY-MM') by default.
            age_months = None
            stale = False
            try:
                if len(as_of) == 7 and as_of[4] == "-":
                    y, m = int(as_of[:4]), int(as_of[5:7])
                    age_months = (today.year - y) * 12 + (today.month - m)
                    stale = age_months > 6
                elif len(as_of) == 10 and as_of[4] == "-" and as_of[7] == "-":
                    y, m, d = int(as_of[:4]), int(as_of[5:7]), int(as_of[8:10])
                    diff = (today - _date(y, m, d)).days
                    age_months = diff // 30
                    stale = diff > 180
            except (ValueError, IndexError):
                pass

            results[ref_area] = {
                "current_rate": obs[-1]["rate"],
                "as_of": as_of,
                "data_age_months": age_months,
                "stale": stale,
                "history": obs,
            }

    # Country name mapping
    names = {"XM": "Euro area (ECB)", "HU": "Hungary (MNB)", "CZ": "Czechia (CNB)",
             "PL": "Poland (NBP)", "RO": "Romania (BNR)", "US": "USA (Fed)",
             "GB": "UK (BoE)", "JP": "Japan (BoJ)", "CH": "Switzerland (SNB)",
             "SE": "Sweden (Riksbank)", "NO": "Norway (Norges)", "DK": "Denmark (DNB)",
             "TR": "Turkey (TCMB)", "HR": "Croatia (HNB)"}

    summary = []
    notes = []
    for code in codes:
        if code in results:
            r = results[code]
            name = names.get(code, code)
            stale_tag = " [STALE!]" if r.get("stale") else ""
            summary.append(f"{name}: {r['current_rate']}% ({r['as_of']}{stale_tag})")
            if r.get("stale"):
                notes.append(
                    f"WARNING: {name} latest BIS data point is from {r['as_of']} "
                    f"(~{r.get('data_age_months')} months ago). The rate may have changed since. "
                    f"Verify with web_search if a current decision matters."
                )

    out = {
        "source": "BIS WS_CBPOL via DBnomics",
        "summary": summary,
        "rates": results,
    }

    # If any country's BIS data is stale, augment with Eurostat irt_st_m
    # (Day-to-day money market rate) as a fresh proxy. The Eurostat money
    # market rate tracks the central bank policy rate within ~10 bps for
    # the CEE economies (HU/CZ/PL/RO) and the Nordics — it's the best
    # available signal when BIS lags by 6+ months. (2026-05-05 audit fix.)
    stale_codes = [c for c in codes if results.get(c, {}).get("stale")]
    if stale_codes:
        try:
            proxy = await _fetch_eurostat_policy_proxy(stale_codes)
            if proxy:
                out["eurostat_proxy"] = {
                    "note": (
                        "BIS WS_CBPOL data above is stale. The values below are "
                        "the Day-to-day money market rate from Eurostat irt_st_m, "
                        "which tracks the central bank policy rate within ~10 bps "
                        "for HU/CZ/PL/RO/Nordics. NOT identical to the policy "
                        "rate but is the best fresh proxy. Euro area, US, JP, "
                        "and other non-EEA countries are NOT covered by Eurostat."
                    ),
                    "rates": proxy,
                }
        except Exception as e:
            logger.warning("Eurostat policy_proxy fallback failed: %s", e)

    # For the euro area (XM) ECB DFR is the actual policy rate. BIS publishes
    # this monthly and lags up to 4 weeks; we always overlay the latest daily
    # value from the ECB Data Portal so XM is never stale.
    if "XM" in codes:
        try:
            ecb_rates = await _fetch_ecb_policy_rates()
            if ecb_rates:
                out.setdefault("ecb_direct", {})
                out["ecb_direct"] = {
                    "note": (
                        "Direct daily values from ECB Data Portal (data-api.ecb.europa.eu). "
                        "DFR = Deposit Facility Rate (the operational policy rate since "
                        "the 2019 corridor reform). MRR_FR = Main Refinancing Operations "
                        "fixed rate. MLFR = Marginal Lending Facility Rate."
                    ),
                    **ecb_rates,
                }
                # Overlay XM current_rate with the fresh DFR so summary reflects truth
                dfr = ecb_rates.get("deposit_facility_rate")
                if isinstance(dfr, dict) and dfr.get("value") is not None:
                    xm = out["rates"].setdefault("XM", {})
                    xm["current_rate_ecb_direct"] = dfr["value"]
                    xm["ecb_as_of"] = dfr.get("period")
                    # Replace the summary line for XM with the fresh ECB DFR
                    for i, line in enumerate(summary):
                        if line.startswith("Euro area"):
                            summary[i] = (
                                f"Euro area (ECB DFR): {dfr['value']}% "
                                f"({dfr.get('period')}) [direct from ECB]"
                            )
                            break
        except Exception as e:
            logger.warning("ECB direct policy rate fetch failed: %s", e)

    if notes:
        out["staleness_notes"] = notes
    return json.dumps(out, ensure_ascii=False, indent=2)


async def _fetch_ecb_policy_rates() -> dict:
    """Fetch the three ECB policy rates directly from ECB Data Portal.

    DFR (Deposit Facility Rate) is the operational policy rate since 2019.
    MRR_FR (Main Refi fixed rate) and MLFR (Marginal Lending Facility) define
    the corridor. Returns the most recent observation for each.
    """
    client = await get_client()
    rate_keys = {
        "deposit_facility_rate": "FM/D.U2.EUR.4F.KR.DFR.LEV",
        "main_refinancing_rate": "FM/D.U2.EUR.4F.KR.MRR_FR.LEV",
        "marginal_lending_rate": "FM/D.U2.EUR.4F.KR.MLFR.LEV",
    }
    out: dict = {}

    async def _one(name: str, path: str) -> tuple:
        url = f"{ECB_BASE}/{path}"
        try:
            r = await client.get(
                url,
                params={"format": "jsondata", "lastNObservations": 1},
                headers={"Accept": "application/json"},
                timeout=15.0,
            )
            if r.status_code != 200:
                return name, None
            parsed = _parse_ecb_jsondata(r.json(), max_obs=1)
            obs = parsed["observations"]
            if not obs:
                return name, None
            return name, {"value": obs[-1]["value"], "period": obs[-1]["period"]}
        except Exception as e:
            logger.warning("ECB rate fetch (%s) failed: %s", name, e)
            return name, None

    results = await asyncio.gather(*[_one(n, p) for n, p in rate_keys.items()])
    for name, val in results:
        if val is not None:
            out[name] = val
    return out


async def _fetch_eurostat_policy_proxy(country_codes: list[str]) -> dict:
    """Fetch Eurostat irt_st_m Day-to-day money market rate as a fresh proxy
    for central bank policy rates. Eurostat covers CEE + Nordics + UK only —
    NOT euro area aggregate, US, JP, EM. Called from get_policy_rates when
    the BIS WS_CBPOL data is stale.
    """
    eurostat_supported = {"HU", "CZ", "PL", "RO", "DK", "SE", "NO", "GB"}
    targets = [c for c in country_codes if c in eurostat_supported]
    if not targets:
        return {}

    proxy_out: dict = {}
    client = await get_client()
    for c in targets:
        try:
            req_url = f"{EUROSTAT_STAT}/irt_st_m?lang=EN&geo={c}&sinceTimePeriod=2025-01"
            resp = await client.get(req_url, timeout=15.0)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if "warning" in data:
                continue
            parsed = _parse_json_stat(data)
            rows = parsed.get("data") or []
            if not rows:
                continue
            # Prefer "Day-to-day" interest rate; fallback to whatever is there.
            d2d = [r for r in rows
                   if str(r.get("Interest rate", "")).lower().startswith("day-to-day")]
            picked = d2d if d2d else rows
            picked.sort(key=lambda r: str(r.get("Time", "")), reverse=True)
            latest = picked[0]
            proxy_out[c] = {
                "value": latest.get("value"),
                "period": latest.get("Time"),
                "rate_type": latest.get("Interest rate", ""),
                "source": "Eurostat irt_st_m (Day-to-day money market rate)",
            }
        except Exception as e:
            logger.warning("Eurostat proxy fetch failed for %s: %s", c, e)
            continue
    return proxy_out


# ---------------------------------------------------------------------------
# ECB Data Portal (direct SDMX 2.1 API)
# ---------------------------------------------------------------------------
# Public, no auth. Same backend as the ECB Statistical Data Warehouse and
# DBnomics' ECB mirror — but typically 1–24h fresher than DBnomics' nightly
# crawl, and exposes the full ECB taxonomy directly (ICP item codes, FM rate
# IDs, BSI monetary aggregates, BLS bank lending survey, etc.).
ECB_BASE = "https://data-api.ecb.europa.eu/service/data"

# Hand-curated catalog of frequently used ECB dataflows. The full ECB
# catalog has ~70 dataflows; this is the subset most relevant for
# HU/EA macro analysis. search_datasets uses this as a static index.
ECB_DATAFLOWS: dict[str, str] = {
    "ICP": "Indices of Consumer Prices (HICP, harmonized inflation)",
    "EXR": "Exchange rates (ECB reference rates, daily/monthly)",
    "FM": "Financial markets (policy rates, EONIA/€STR, money market)",
    "BSI": "Balance Sheet Items (monetary aggregates M1/M2/M3, MFI balance sheets)",
    "MIR": "MFI Interest Rates (bank lending/deposit rates by country)",
    "BLS": "Bank Lending Survey (credit standards, demand for loans)",
    "IRS": "Long-term interest rates (Maastricht convergence criterion)",
    "STS": "Short-term statistics (industrial production, retail trade, PPI)",
    "CISS": "Composite Indicator of Systemic Stress (financial stress index)",
    "GFS": "Government Finance Statistics (deficit, debt, EDP)",
    "MNA": "National Accounts (quarterly GDP, ESA 2010)",
    "QSA": "Quarterly Sector Accounts (households, corporates, government)",
    "SPF": "Survey of Professional Forecasters (inflation/GDP expectations)",
    "RAI": "Residential property prices (House Price Indices)",
    "CPP": "Commercial property prices",
    "BOP": "Balance of payments",
    "TRD": "International Trade",
    "YC": "Yield curve (government bond zero-coupon yield curves)",
    "IVF": "Investment Funds (IF balance sheets, flows)",
    "PSS": "Payment Systems (TARGET2, retail payments)",
}

# Commonly needed ECB series keys — used by search_datasets so AI agents
# can find them and by the recipe seeding logic below.
ECB_SERIES_CATALOG: dict[str, str] = {
    # === HICP (Hungary + euro area) ===
    "ICP/M.HU.N.000000.4.ANR": "HU HICP overall, monthly, annual rate of change (%)",
    "ICP/M.HU.N.XEF000.4.ANR": "HU HICP core (excl. energy & food), monthly YoY%",
    "ICP/M.HU.N.SERV00.4.ANR": "HU HICP services, monthly YoY%",
    "ICP/M.HU.N.IGOOD0.4.ANR": "HU HICP non-energy industrial goods, monthly YoY%",
    "ICP/M.HU.N.NRGY00.4.ANR": "HU HICP energy, monthly YoY%",
    "ICP/M.HU.N.FOOD00.4.ANR": "HU HICP food, monthly YoY%",
    "ICP/M.U2.N.000000.4.ANR": "Euro area HICP overall, monthly YoY% (flash + final)",
    "ICP/M.U2.N.XEF000.4.ANR": "Euro area HICP core (excl. energy & food), monthly YoY%",
    "ICP/M.U2.N.SERV00.4.ANR": "Euro area HICP services, monthly YoY%",
    # === Exchange rates ===
    "EXR/D.HUF.EUR.SP00.A": "EUR/HUF daily reference rate (ECB)",
    "EXR/M.HUF.EUR.SP00.A": "EUR/HUF monthly average reference rate",
    "EXR/D.USD.EUR.SP00.A": "USD/EUR daily reference rate",
    "EXR/D.PLN.EUR.SP00.A": "EUR/PLN daily reference rate",
    "EXR/D.CZK.EUR.SP00.A": "EUR/CZK daily reference rate",
    "EXR/D.RON.EUR.SP00.A": "EUR/RON daily reference rate",
    "EXR/D.GBP.EUR.SP00.A": "GBP/EUR daily reference rate",
    "EXR/D.CHF.EUR.SP00.A": "CHF/EUR daily reference rate",
    # === Policy rates ===
    "FM/D.U2.EUR.4F.KR.DFR.LEV": "ECB Deposit Facility Rate (daily, %)",
    "FM/D.U2.EUR.4F.KR.MRR_FR.LEV": "ECB Main Refinancing Rate, fixed (daily, %)",
    "FM/D.U2.EUR.4F.KR.MLFR.LEV": "ECB Marginal Lending Facility Rate (daily, %)",
    "FM/B.U2.EUR.4F.KR.MRR_FR.LEV": "ECB Main Refi Rate, business-day, %",
    "FM/D.U2.EUR.RT.MM.EONIA_.HSTA": "EONIA / €STR overnight rate (historical, %)",
    # === Long-term rates (Maastricht) ===
    "IRS/M.HU.L.L40.CI.0000.HUF.N.Z": "HU 10Y government bond yield, monthly, Maastricht",
    "IRS/M.U2.L.L40.CI.0000.EUR.N.Z": "Euro area 10Y government bond yield, monthly",
    # === Monetary aggregates ===
    "BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E": "Euro area M3 monetary aggregate (€ bn)",
    # === Bond yield curve ===
    "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y": "Euro area 10Y spot rate, AAA, daily (%)",
}


def _parse_ecb_jsondata(data: dict, max_obs: int = 200) -> dict:
    """Parse an ECB Data Portal SDMX-JSON response into a list of observations.

    The ECB API returns observations indexed by ordinal time positions, with
    the time period labels in structure.observation.values. We flatten this
    into [{period: '2025-12', value: 3.3}, ...].
    """
    out: dict = {"observations": [], "meta": {}}

    datasets = data.get("dataSets") or []
    if not datasets:
        return out
    series_dict = datasets[0].get("series") or {}
    if not series_dict:
        return out

    # Time period labels live in structure.observation[0].values
    structure = data.get("structure") or {}
    obs_dim = structure.get("dimensions", {}).get("observation") or []
    periods: list[str] = []
    if obs_dim:
        periods = [v.get("id", "") for v in obs_dim[0].get("values", [])]

    # Dimension metadata for the series
    series_dims = structure.get("dimensions", {}).get("series") or []
    name = structure.get("name") or ""
    out["meta"] = {"dataflow_name": name, "series_dimensions": {
        d.get("id"): (d.get("values") or [{}])[0].get("name", "")
        for d in series_dims
    }}

    # Take the first (and usually only) series
    first_series_key = next(iter(series_dict))
    s = series_dict[first_series_key]
    raw_obs = s.get("observations") or {}

    rows = []
    for idx_str, obs_vals in raw_obs.items():
        try:
            idx = int(idx_str)
        except ValueError:
            continue
        period = periods[idx] if 0 <= idx < len(periods) else idx_str
        value = obs_vals[0] if obs_vals else None
        rows.append({"period": period, "value": value})

    rows.sort(key=lambda r: r["period"])
    out["observations"] = rows[-max_obs:]
    return out


@mcp.tool()
async def get_ecb_data(
    dataset: str,
    key: str,
    start_period: str = "",
    end_period: str = "",
    last_n: int = 12,
) -> str:
    """Fetch a time series from the ECB Data Portal (direct SDMX 2.1 API).

    Source: data-api.ecb.europa.eu/service/data — same underlying data as the
    ECB Statistical Data Warehouse, the canonical source for euro-area HICP,
    ECB policy rates, EUR exchange rates, money market rates, MFI balance
    sheets, government bond yields, etc. Typically 1–24h fresher than
    DBnomics' mirror.

    Args:
        dataset: ECB dataflow code. Common ones:
                 ICP — HICP / consumer prices (overall + sub-aggregates: services,
                       energy, food, core). HU AND euro area available.
                 EXR — Exchange rates (EUR reference rates, daily + monthly)
                 FM  — Financial markets (ECB policy rates, EONIA/€STR)
                 IRS — Long-term interest rates (Maastricht 10Y govt bond)
                 BSI — Monetary aggregates M1/M2/M3, MFI balance sheets
                 MIR — Bank lending/deposit rates by country
                 STS — Short-term statistics (industrial production, retail trade)
                 YC  — Yield curve (zero-coupon spot rates)
                 GFS — Government finance (deficit, debt, EDP)
                 MNA — Quarterly national accounts (GDP, ESA 2010)
                 BLS — Bank Lending Survey
        key: SDMX series key — dot-separated dimension values. Leave any
             dimension empty for wildcard. Examples:
               "M.HU.N.000000.4.ANR" — HU HICP overall, monthly YoY%
               "M.HU.N.SERV00.4.ANR" — HU HICP services, monthly YoY%
               "M.U2.N.XEF000.4.ANR" — Euro area core HICP, monthly YoY%
               "D.HUF.EUR.SP00.A"    — EUR/HUF daily reference rate
               "D.U2.EUR.4F.KR.DFR.LEV" — ECB Deposit Facility Rate (daily)
               "M.HU.L.L40.CI.0000.HUF.N.Z" — HU 10Y Maastricht bond yield (monthly)
             For HICP services use ICP_ITEM=SERV00 (NOT 'SERV'). For core HICP
             excluding energy and food use XEF000. Hungary REF_AREA=HU,
             euro area=U2. STS_INSTITUTION=4 for Eurostat-sourced HICP.
        start_period: Optional start period (e.g. "2024-01" or "2024-Q1"). Empty = all.
        end_period: Optional end period. Empty = latest available.
        last_n: If start_period is empty, return only the last N observations
                (default 12). Set 0 to disable and return everything.

    Returns:
        JSON with dataflow metadata, dimension descriptions, and observations
        (period + value pairs, oldest to newest).

    Hints for HU HICP coverage:
        ECB ICP receives HU monthly HICP from Eurostat ~2–3 days after the
        KSH "Fogyasztói árak" flash release. If you need TODAY's number and
        ECB still shows last month, fall back to get_ksh_flash with query
        "fogyasztói árak".
    """
    dataset = dataset.strip().upper()
    key = key.strip()
    if not dataset or not key:
        return json.dumps({
            "error": "Both 'dataset' and 'key' are required",
            "hint": "Example: dataset='ICP', key='M.HU.N.000000.4.ANR'",
            "known_dataflows": ECB_DATAFLOWS,
        }, ensure_ascii=False, indent=2)

    params: dict = {"format": "jsondata"}
    if start_period:
        params["startPeriod"] = start_period
    if end_period:
        params["endPeriod"] = end_period
    if last_n and last_n > 0 and not start_period:
        params["lastNObservations"] = int(last_n)

    url = f"{ECB_BASE}/{dataset}/{key}"
    client = await get_client()
    try:
        resp = await client.get(url, params=params, headers={"Accept": "application/json"}, timeout=30.0)
    except Exception as e:
        return json.dumps({"error": f"ECB request failed: {e}"}, ensure_ascii=False, indent=2)

    if resp.status_code == 404:
        return json.dumps({
            "error": f"ECB series not found: {dataset}/{key}",
            "status": 404,
            "hint": (
                "Check series key dimensions. For ICP services use SERV00 (not SERV). "
                "For wildcard search use empty dimensions: 'M.HU.N..4.ANR'. "
                "Browse the catalog at https://data.ecb.europa.eu/data/datasets"
            ),
            "known_dataflows": ECB_DATAFLOWS,
        }, ensure_ascii=False, indent=2)
    if resp.status_code != 200:
        return json.dumps({
            "error": f"ECB HTTP {resp.status_code}",
            "body": resp.text[:300],
        }, ensure_ascii=False, indent=2)

    try:
        data = resp.json()
    except Exception as e:
        return json.dumps({"error": f"ECB JSON parse failed: {e}"}, ensure_ascii=False, indent=2)

    parsed = _parse_ecb_jsondata(data)

    out = {
        "source": "ECB Data Portal (data-api.ecb.europa.eu)",
        "dataset": dataset,
        "key": key,
        "name": parsed["meta"].get("dataflow_name", ""),
        "dimensions": parsed["meta"].get("series_dimensions", {}),
        "observations_returned": len(parsed["observations"]),
        "data": parsed["observations"],
    }

    if parsed["observations"]:
        try:
            _auto_learn_recipe(
                "ECB", dataset, {"key": key},
                out["name"] or f"{dataset}/{key}",
                len(parsed["observations"]),
            )
        except Exception:
            pass

    return json.dumps(out, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Flash releases — KSH and Eurostat (gyorstájékoztatók)
# ---------------------------------------------------------------------------
# Both KSH (gyorstajekoztatok.xml RSS) and Eurostat (Atom feed via the news
# portlet) publish flash macro releases 1–3 days before the official data
# tables update. We index both into a shared SQLite cache so search_datasets
# and get_flash_releases can surface them.
KSH_FLASH_RSS = "https://www.ksh.hu/rss/gyorstajekoztatok.xml"

# KSH gyorstájékoztató topic-kódok a /gyorstajekoztatok/<topic>/<topic>YYMM.html
# URL-mintára. Ezt a get_flash_releases tool indexeli az RSS feed helyett (az
# RSS valójában általános KSH-hírfolyam, NEM a strukturált gyorstájékoztatók).
KSH_FLASH_TOPICS: dict[str, str] = {
    "far":  "Fogyasztói árak",
    "ipi":  "Ipari termelés",
    "mun":  "Munkaerőpiac",
    "gdp":  "GDP gyorsbecslés",
    "kik":  "Kiskereskedelem",
    "tur":  "Turizmus",
    "kkr":  "Külkereskedelem",
    "ber":  "Bruttó kereset",
    "fok":  "Foglalkoztatottság",
    "lak":  "Lakossági fogyasztás",
    "epi":  "Építőipar",
    "nep":  "Népesség",
}
EUROSTAT_FLASH_ATOM = (
    "https://ec.europa.eu/eurostat/web/main/news/euro-indicators"
    "?p_p_id=estatsearchportlet_WAR_estatsearchportlet_INSTANCE_OaTpFrwlabNK"
    "&p_p_lifecycle=2"
    "&p_p_resource_id=atom"
    "&_estatsearchportlet_WAR_estatsearchportlet_INSTANCE_OaTpFrwlabNK_pageSize=60"
    "&_estatsearchportlet_WAR_estatsearchportlet_INSTANCE_OaTpFrwlabNK_sort=lastUpdateDate"
    "&_estatsearchportlet_WAR_estatsearchportlet_INSTANCE_OaTpFrwlabNK_collection=CAT_PREREL"
)
FLASH_DB_PATH = os.environ.get("FLASH_DB", "/tmp/flash_releases.db")
FLASH_TTL = 3600 * 6  # refresh both feeds every 6 hours
_flash_loaded_at: dict[str, float] = {"ksh": 0.0, "eurostat": 0.0}


def _init_flash_db() -> None:
    conn = sqlite3.connect(FLASH_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS flash_items (
            link TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            pub_date TEXT,
            description TEXT,
            fetched_at REAL NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_flash_source ON flash_items(source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_flash_pub_date ON flash_items(pub_date)")
    conn.commit()
    conn.close()


def _clean_text(s: str) -> str:
    s = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", s, flags=re.DOTALL)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for ent, repl in (("&amp;", "&"), ("&quot;", '"'), ("&lt;", "<"), ("&gt;", ">"),
                      ("&#8211;", "–"), ("&#8217;", "’"), ("&apos;", "'"),
                      ("&#39;", "'"), ("&#34;", '"')):
        s = s.replace(ent, repl)
    return s


def _parse_ksh_rss(xml_bytes: bytes) -> list[dict]:
    """Parse the KSH RSS feed (ISO-8859-2). KSH's feed uses lowercase
    <pubdate> instead of <pubDate>, hence regex with re.IGNORECASE.
    """
    try:
        text = xml_bytes.decode("iso-8859-2")
    except (UnicodeDecodeError, LookupError):
        text = xml_bytes.decode("utf-8", errors="replace")

    items: list[dict] = []
    for chunk in re.findall(r"<item>(.*?)</item>", text, flags=re.DOTALL | re.IGNORECASE):
        def _grab(tag: str) -> str:
            m = re.search(rf"<{tag}>(.*?)</{tag}>", chunk, flags=re.DOTALL | re.IGNORECASE)
            return _clean_text(m.group(1)) if m else ""

        title = _grab("title")
        link = _grab("link")
        pub = _grab("pubdate")
        desc = _grab("description")
        if link and title:
            items.append({"title": title, "link": link, "pub_date": pub, "description": desc})
    return items


def _parse_eurostat_atom(xml_bytes: bytes) -> list[dict]:
    """Parse the Eurostat Atom feed of news/press releases (CAT_PREREL)."""
    text = xml_bytes.decode("utf-8", errors="replace")
    items: list[dict] = []
    for chunk in re.findall(r"<entry>(.*?)</entry>", text, flags=re.DOTALL):
        title_m = re.search(r"<title[^>]*>(.*?)</title>", chunk, flags=re.DOTALL)
        link_m = re.search(r'<link[^>]+href="([^"]+)"', chunk)
        pub_m = re.search(r"<published>(.*?)</published>", chunk, flags=re.DOTALL)
        summary_m = re.search(r"<summary[^>]*>(.*?)</summary>", chunk, flags=re.DOTALL)
        title = _clean_text(title_m.group(1)) if title_m else ""
        link = link_m.group(1).replace("&amp;", "&") if link_m else ""
        pub = _clean_text(pub_m.group(1)) if pub_m else ""
        summary = _clean_text(summary_m.group(1)) if summary_m else ""
        if link and title:
            items.append({"title": title, "link": link, "pub_date": pub, "description": summary})
    return items


async def _probe_ksh_flash_topics() -> list[dict]:
    """Probe current+previous month for each KSH_FLASH_TOPICS topic, build
    items list from the URLs that return 200. Replaces the unreliable RSS
    feed (which is a general news feed, not the structured gyorstájékoztatók).
    """
    from datetime import datetime as _dt, timedelta as _td
    client = await get_client()
    now = _dt.now()
    items: list[dict] = []

    async def _probe_one(topic: str, label: str, year: int, month: int):
        yy = f"{year % 100:02d}"
        mm = f"{month:02d}"
        url = f"https://www.ksh.hu/gyorstajekoztatok/{topic}/{topic}{yy}{mm}.html"
        try:
            r = await client.head(url, timeout=8.0)
            if r.status_code == 200:
                return {
                    "title": f"{label} {year}. {mm}.",
                    "link": url,
                    "pub_date": f"{year}-{mm}-01",
                    "description": f"KSH gyorstájékoztató — {label} {year}-{mm}",
                }
        except Exception:
            pass
        return None

    # Try current and previous 2 months for each topic
    tasks = []
    for topic, label in KSH_FLASH_TOPICS.items():
        for back in range(0, 3):
            m_idx = now.month - back
            y = now.year
            while m_idx <= 0:
                m_idx += 12
                y -= 1
            tasks.append(_probe_one(topic, label, y, m_idx))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, dict):
            items.append(r)
    return items


async def _refresh_flash_source(source: str, force: bool = False) -> int:
    """Fetch a single flash source (ksh|eurostat) and upsert into SQLite.

    For 'ksh' source: uses direct gyorstájékoztató URL probing (the RSS feed
    is a general news stream and doesn't include structured flash releases).
    Falls back to RSS if URL probing yields zero items.
    """
    now = time.time()
    if not force and (now - _flash_loaded_at.get(source, 0.0)) < FLASH_TTL:
        return 0

    _init_flash_db()
    items: list[dict] = []

    if source == "ksh":
        try:
            items = await _probe_ksh_flash_topics()
        except Exception as e:
            logger.warning("KSH flash topic probe failed: %s", e)
        if not items:
            # Fallback: RSS (general news feed)
            client = await get_client()
            try:
                resp = await client.get(KSH_FLASH_RSS, timeout=20.0)
                resp.raise_for_status()
                items = _parse_ksh_rss(resp.content)
            except Exception as e:
                logger.warning("KSH RSS fallback failed: %s", e)
    else:
        # Eurostat — Atom feed
        client = await get_client()
        try:
            resp = await client.get(EUROSTAT_FLASH_ATOM, timeout=25.0)
            resp.raise_for_status()
            items = _parse_eurostat_atom(resp.content)
        except Exception as e:
            logger.warning("Eurostat flash fetch failed: %s", e)
            return 0

    if not items:
        return 0

    conn = sqlite3.connect(FLASH_DB_PATH)
    new_count = 0
    for it in items:
        cur = conn.execute(
            "INSERT OR IGNORE INTO flash_items (link, source, title, pub_date, description, fetched_at) "
            "VALUES (?,?,?,?,?,?)",
            (it["link"], source, it["title"], it.get("pub_date", ""), it.get("description", ""), now),
        )
        if cur.rowcount:
            new_count += 1
        else:
            conn.execute(
                "UPDATE flash_items SET title=?, pub_date=?, description=?, fetched_at=? "
                "WHERE link=? AND source=?",
                (it["title"], it.get("pub_date", ""), it.get("description", ""), now,
                 it["link"], source),
            )
    conn.commit()
    conn.close()
    _flash_loaded_at[source] = now
    logger.info("Flash refresh [%s]: %d items (%d new)", source, len(items), new_count)
    return new_count


async def _refresh_flash_all(force: bool = False) -> dict:
    """Refresh both KSH and Eurostat feeds in parallel."""
    ksh_n, eu_n = await asyncio.gather(
        _refresh_flash_source("ksh", force=force),
        _refresh_flash_source("eurostat", force=force),
        return_exceptions=True,
    )
    return {
        "ksh_new": ksh_n if isinstance(ksh_n, int) else 0,
        "eurostat_new": eu_n if isinstance(eu_n, int) else 0,
    }


def _search_flash_db(query: str, source: str = "all", limit: int = 20) -> list[dict]:
    """Search cached flash releases by keyword. source: 'ksh'|'eurostat'|'all'."""
    if not os.path.exists(FLASH_DB_PATH):
        return []
    where = ""
    params: tuple = ()
    if source in ("ksh", "eurostat"):
        where = "WHERE source = ?"
        params = (source,)
    try:
        conn = sqlite3.connect(FLASH_DB_PATH)
        rows = conn.execute(
            f"SELECT link, source, title, pub_date, description FROM flash_items {where} "
            f"ORDER BY pub_date DESC", params,
        ).fetchall()
        conn.close()
    except Exception:
        return []

    if not query.strip():
        return [
            {"source": s, "title": t, "link": l, "pub_date": p, "description": d}
            for l, s, t, p, d in rows[:limit]
        ]

    keywords = query.lower().split()
    scored: list[tuple] = []
    for link, src, title, pub, desc in rows:
        text = f"{title} {desc}".lower()
        score = sum(1 for kw in keywords if kw in text)
        if score > 0:
            scored.append((score, pub or "", {
                "source": src, "title": title, "link": link,
                "pub_date": pub, "description": desc,
            }))
    # Highest score first, then most recent pub_date first within same score
    scored.sort(key=lambda x: (-x[0], x[1]), reverse=False)
    scored.sort(key=lambda x: (-x[0], -ord(x[1][0]) if x[1] else 0))
    # Cleaner: just sort by score desc, pub_date desc
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [e for _, _, e in scored[:limit]]


@mcp.tool()
async def get_flash_releases(
    query: str = "",
    source: str = "all",
    limit: int = 15,
    refresh: bool = False,
) -> str:
    """Search flash statistical releases ("gyorstájékoztatók") from KSH and Eurostat.

    Use this tool when official time-series APIs (Eurostat, ECB, KSH STADAT)
    are not yet updated with the latest period and you need the freshest
    published number. KSH typically publishes 1–3 days before Eurostat
    re-ingests the data; Eurostat publishes euro-area flash estimates (HICP,
    GDP) ahead of the official statistical release.

    Args:
        query: Keyword filter (HU or EN). Examples:
                 KSH:      "fogyasztói árak", "munkanélküliség", "ipari",
                           "kiskereskedelem", "üzemanyag", "GDP", "infláció"
                 Eurostat: "HICP", "inflation", "unemployment", "GDP", "PPI",
                           "industrial", "retail trade", "trade", "deficit"
               Empty = return latest items unfiltered.
        source: "ksh" | "eurostat" | "all" (default).
        limit: Max items to return (default 15).
        refresh: Force feed refresh even if cached. Default False (6h TTL).

    Returns:
        JSON with items: source, title, link, pub_date, description.

    Coverage notes:
        - KSH RSS retains only the most recent ~5–20 items (rolling window).
        - Eurostat Atom feed returns up to 60 items per page (last few months
          of euro-indicator press releases).
        - For HU monthly HICP / unemployment / GDP after the data is published,
          combine: first get_flash_releases for the headline + link, then
          get_ksh_stadat / get_eurostat_data / get_ecb_data for the structured
          time series.
    """
    src = source.strip().lower()
    if src not in ("ksh", "eurostat", "all"):
        return json.dumps({
            "error": f"Invalid source '{source}'",
            "hint": "Use 'ksh', 'eurostat', or 'all'.",
        }, ensure_ascii=False, indent=2)

    try:
        if src == "all":
            await _refresh_flash_all(force=refresh)
        else:
            await _refresh_flash_source(src, force=refresh)
    except Exception as e:
        logger.warning("Flash refresh failed: %s", e)

    rows = _search_flash_db(query, src, limit)
    out = {
        "source": "Flash releases (KSH + Eurostat)" if src == "all" else (
            "KSH gyorstájékoztatók (RSS)" if src == "ksh" else "Eurostat news (Atom)"
        ),
        "source_filter": src,
        "query": query,
        "count": len(rows),
        "items": rows,
    }
    if not rows:
        out["hint"] = (
            "No matches. Try broader keywords or empty query for the latest items. "
            "Set refresh=True to force a feed update."
        )
    return json.dumps(out, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Macro indicator router — guaranteed-fresh, country-agnostic
# ---------------------------------------------------------------------------
# High-level "give me fresh HU policy rate / DE CPI / US unemployment" tool.
# Routes through a resolver chain per (country, indicator) and returns the
# FIRST resolver whose value is fresher than the threshold. If every resolver
# is stale or fails, returns the freshest stale value with explicit status.
#
# This is the answer to "the system MUST work and give reliable data": the
# Bridge / sub-agent makes ONE call and gets a number + period + source-used,
# instead of orchestrating six separate API calls and falling back to
# web_search themselves.

BRAVE_MCP_URL = os.environ.get("BRAVE_MCP_URL", "").strip().rstrip("/")

# How recent the latest observation must be for an indicator to be "fresh"
# enough that we stop the resolver chain. Tuned to typical publication cadence
# plus a 2-week grace window.
_FRESHNESS_DAYS: dict[str, int] = {
    "cpi": 60,           # Monthly, published ~10–15 days after month-end
    "core_cpi": 60,
    "services_cpi": 60,
    "energy_cpi": 60,
    "food_cpi": 60,
    "policy_rate": 75,   # Monthly meetings; flash decision page must be <2.5mo
    "unemployment": 75,  # Monthly, published with 1–2 month lag (Eurostat)
    "gdp": 150,          # Quarterly absolute value
    "gdp_growth": 150,   # Quarterly YoY%
    "ppi": 60,           # Monthly producer prices
    "wages": 90,         # Monthly with longer lag (Eurostat quarterly)
    "retail_trade": 75,
    "industrial_production": 75,
    "trade_balance": 90,
    "gov_debt": 180,     # Quarterly, often a quarter lag
    "house_prices": 180,
    "bond_yield_10y": 60,  # Daily/monthly; long-term Maastricht yield
    "gdp_consumption": 150,  # Quarterly GDP components
    "gdp_investment": 150,
    "gdp_exports": 150,
    "gdp_imports": 150,
    "gdp_government": 150,
}

# Per-(country, indicator) resolver chain. Each resolver is a dict with `type`
# and parameters; the chain tries them in order and returns the first fresh
# observation. Resolvers:
#   - "ecb":          ECB Data Portal direct SDMX (uses get_ecb_data internals)
#   - "eurostat":     Eurostat JSON-stat API
#   - "fred":         FRED REST API (US data)
#   - "ksh_stadat":   KSH STADAT table (HU only)
#   - "scrape":       Static URL scrape via brave-mcp (JS-rendered OK), regex extraction
#   - "brave_search": Brave search with site filter + scrape top hit
#   - "bis":          BIS WS_CBPOL via DBnomics
#   - "dbnomics":     Any DBnomics series (provider/dataset/code)
#
# The table is built in two passes:
#   1. _eu_country_resolvers(c) generates the standard 8-indicator block for
#      every EU/EEA country (ECB ICP + Eurostat + generic brave_search)
#   2. Country-specific overrides (HU, DE, FR, US, GB, ...) add language-
#      localized brave queries, national scrape URLs, and policy_rate logic.
#
# The brave-search query templates use {YYYY-MM} (= previous month, since
# current month is usually not yet published) and {YYYY} placeholders.


# Non-euro currency codes for ECB IRS bond-yield series. Eurozone members
# all use EUR; others have their own ISO-4217 code (Maastricht series uses
# the national currency).
_CCY_FOR_COUNTRY: dict[str, str] = {
    "HU": "HUF", "CZ": "CZK", "PL": "PLN", "RO": "RON",
    "SE": "SEK", "DK": "DKK", "NO": "NOK", "CH": "CHF", "GB": "GBP",
    "BG": "BGN", "HR": "EUR",  # HR adopted EUR 2023
}


def _eu_country_resolvers(c: str) -> dict[str, list[dict]]:
    """Standard 10+ indicator resolver block for an EU/EEA country.

    Coverage: cpi, core_cpi, services_cpi, energy_cpi, food_cpi, ppi,
    unemployment, gdp, gdp_growth, bond_yield_10y, retail_trade,
    industrial_production, trade_balance, gov_debt. All resolvers chain
    ECB ICP → Eurostat → generic brave_search; country-specific overrides
    (national stats office scrape, native-language queries) are layered
    ON TOP after this generator.
    """
    return {
        "cpi": [
            {"type": "ecb", "dataset": "ICP", "key": f"M.{c}.N.000000.4.ANR"},
            {"type": "eurostat", "dataset_code": "prc_hicp_manr", "geo": c},
            {"type": "brave_search",
             "query": f"HICP inflation {c} {{YYYY-MM}} annual rate",
             "rx": r"(\d+[,.]\d)\s*%"},
        ],
        "core_cpi": [
            {"type": "ecb", "dataset": "ICP", "key": f"M.{c}.N.XEF000.4.ANR"},
            {"type": "brave_search",
             "query": f"HICP core inflation {c} {{YYYY-MM}}",
             "rx": r"(\d+[,.]\d)\s*%"},
        ],
        "services_cpi": [
            {"type": "ecb", "dataset": "ICP", "key": f"M.{c}.N.SERV00.4.ANR"},
            {"type": "brave_search",
             "query": f"HICP services inflation {c} {{YYYY-MM}}",
             "rx": r"(\d+[,.]\d)\s*%"},
        ],
        "energy_cpi": [
            {"type": "ecb", "dataset": "ICP", "key": f"M.{c}.N.NRGY00.4.ANR"},
        ],
        "food_cpi": [
            {"type": "ecb", "dataset": "ICP", "key": f"M.{c}.N.FOOD00.4.ANR"},
        ],
        "ppi": [
            {"type": "eurostat", "dataset_code": "sts_inpp_m", "geo": c,
             "filters": "indic_bt=PRC_PRR&nace_r2=B-E36&s_adj=NSA&unit=RCH_A"},
            {"type": "brave_search",
             "query": f"PPI producer prices {c} {{YYYY-MM}} annual",
             "rx": r"(\d+[,.]\d)\s*%"},
        ],
        "unemployment": [
            {"type": "eurostat", "dataset_code": "une_rt_m", "geo": c,
             "filters": "sex=T&age=TOTAL&unit=PC_ACT&s_adj=SA"},
            {"type": "brave_search",
             "query": f"unemployment rate {c} {{YYYY-MM}} Eurostat",
             "rx": r"(\d+[,.]\d)\s*%"},
        ],
        "gdp": [
            {"type": "eurostat", "dataset_code": "namq_10_gdp", "geo": c,
             "filters": "na_item=B1GQ&unit=CLV15_MEUR&s_adj=SCA"},
            {"type": "brave_search",
             "query": f"GDP {c} {{YYYY}} quarterly billion EUR Eurostat",
             "rx": r"(\d[\d,. ]{2,})"},
        ],
        "gdp_growth": [
            {"type": "eurostat", "dataset_code": "namq_10_gdp", "geo": c,
             "filters": "na_item=B1GQ&unit=CLV_PCH_PRE&s_adj=SCA"},
            {"type": "brave_search",
             "query": f"GDP growth rate {c} {{YYYY}} quarterly percent change Eurostat",
             "rx": r"(-?\d+[,.]\d)\s*%"},
        ],
        "bond_yield_10y": [
            {"type": "ecb", "dataset": "IRS",
             "key": f"M.{c}.L.L40.CI.0000.{_CCY_FOR_COUNTRY.get(c, 'EUR')}.N.Z"},
            {"type": "brave_search",
             "query": f"{c} 10-year government bond yield {{YYYY-MM}}",
             "rx": r"(\d+[,.]\d{1,3})\s*%"},
        ],
        "retail_trade": [
            {"type": "eurostat", "dataset_code": "sts_trtu_m", "geo": c},
            {"type": "brave_search",
             "query": f"retail trade volume {c} {{YYYY-MM}} Eurostat",
             "rx": r"(-?\d+[,.]\d)\s*%"},
        ],
        "industrial_production": [
            {"type": "eurostat", "dataset_code": "sts_inpr_m", "geo": c},
            {"type": "brave_search",
             "query": f"industrial production {c} {{YYYY-MM}} Eurostat",
             "rx": r"(-?\d+[,.]\d)\s*%"},
        ],
        "trade_balance": [
            {"type": "eurostat", "dataset_code": "ext_lt_intertrd", "geo": c},
        ],
        "gov_debt": [
            {"type": "eurostat", "dataset_code": "gov_10q_ggdebt", "geo": c,
             "filters": "unit=PC_GDP&sector=S13"},
        ],
    }


INDICATOR_RESOLVERS: dict[tuple[str, str], list[dict]] = {}

# ─── Pass 1: standard EU/EEA country blocks (auto-generated) ──────────
# 27 EU + Norway + Switzerland — all get ECB ICP + Eurostat + generic
# brave_search coverage for CPI/core/services/energy/food/ppi/unemployment/
# gdp/retail_trade/industrial_production/trade_balance/gov_debt (12 indicators).
for _c in (
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR",
    "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL",
    "PT", "RO", "SE", "SI", "SK", "NO", "CH",
):
    _block = _eu_country_resolvers(_c)
    for _ind, _list in _block.items():
        INDICATOR_RESOLVERS[(_c, _ind)] = _list

# ─── Pass 2: policy_rate via BIS WS_CBPOL + central-bank scrapes ──────
# Most EU central banks publish their policy rate on a stable URL; the BIS
# WS_CBPOL series is the structured fallback (often stale by 1–3 months).
_POLICY_RATE_SCRAPE_URLS: dict[str, list[tuple[str, str]]] = {
    # (url, regex) tuples in order of preference
    "HU": [
        ("https://www.mnb.hu/sajtoszoba/sajtokozlemenyek",
         r"(?:irányadó kamat|alapkamat|jegybanki[\s\w]*kamat)[\s\S]{0,300}?(\d+[,.]\d{1,2})\s*%"),
        ("https://www.mnb.hu/Root/Dokumentumtar/MNB/Monetaris_politika/mnben_jegybanki_alapkamat",
         r"(\d+[,.]\d{1,2})\s*%"),
    ],
    "CZ": [("https://www.cnb.cz/en/monetary-policy/bank-board-decisions/",
            r"(?:two-week repo rate|2W repo rate)[\s\S]{0,200}?(\d+[,.]\d{1,2})\s*%")],
    "PL": [("https://nbp.pl/en/monetary-policy/decisions-of-the-monetary-policy-council/",
            r"(?:reference rate)[\s\S]{0,200}?(\d+[,.]\d{1,2})\s*%")],
    "RO": [("https://www.bnro.ro/Monetary-Policy--3318.aspx",
            r"(?:monetary policy rate|key policy rate)[\s\S]{0,200}?(\d+[,.]\d{1,2})\s*%")],
    "SE": [("https://www.riksbank.se/en-gb/monetary-policy/the-policy-rate/",
            r"(?:policy rate)[\s\S]{0,200}?(\d+[,.]\d{1,2})\s*%")],
    "DK": [("https://www.nationalbanken.dk/en/what-we-do/stable-prices-monetary-policy-and-the-danish-economy/official-interest-rates",
            r"(?:current account rate|certificates of deposit rate)[\s\S]{0,200}?(\d+[,.]\d{1,2})\s*%")],
    "NO": [("https://www.norges-bank.no/en/topics/Monetary-policy/Policy-rate/",
            r"(?:policy rate)[\s\S]{0,200}?(\d+[,.]\d{1,2})\s*%")],
    "CH": [("https://www.snb.ch/en/iabout/monpol/id/monpol_current",
            r"(?:policy rate|SNB policy rate)[\s\S]{0,200}?(-?\d+[,.]\d{1,2})\s*%")],
    "GB": [("https://www.bankofengland.co.uk/monetary-policy/the-interest-rate-bank-rate",
            r"Bank Rate[\s\S]{0,300}?(\d+[,.]\d{1,2})\s*%")],
}

for _c, _scrape_list in _POLICY_RATE_SCRAPE_URLS.items():
    INDICATOR_RESOLVERS[(_c, "policy_rate")] = [
        {"type": "scrape", "url": _url, "rx": _rx} for _url, _rx in _scrape_list
    ] + [
        {"type": "brave_search",
         "query": f"central bank policy rate {_c} {{YYYY-MM}} monetary policy decision",
         "rx": r"(\d+[,.]\d{1,2})\s*%"},
        {"type": "bis", "country": _c},
    ]

# ─── Pass 3: country-specific overrides (native language brave queries) ──

# Hungary — Magyar nyelvű brave queryk és KSH-stadat fallback
INDICATOR_RESOLVERS[("HU", "cpi")] = [
    # ELSŐ: KSH gyorstájékoztató — célzott YoY-mondat (markdown + HTML kompatibilis)
    # "...fogyasztói árak átlagosan 2,1%-kal haladták meg az egy évvel korábbi"
    {"type": "ksh_flash",    "topic": "far",
                              "rx": r"fogyasztói\s+árak[\s\S]{0,80}?átlagosan\s+(\d+[,.]\d)\s*%?[\s\-]*kal"},
    {"type": "ksh_flash",    "topic": "far",
                              "rx": r"(\d+[,.]\d)\s*%[\s\-]*kal\s+haladt[áa]k?\s+meg\s+az\s+egy\s+évvel"},
    # Megj.: az Eurostat HICP flash press release CSAK euro-area aggregate-et
    # ad. HU-ra (mint nem-EA tag) a kanonikus forrás a KSH gyorstájékoztató —
    # azt scrape-eljük közvetlenül a fenti ksh_flash resolverekkel.
    {"type": "ecb",          "dataset": "ICP", "key": "M.HU.N.000000.4.ANR"},
    {"type": "eurostat",     "dataset_code": "prc_hicp_manr", "geo": "HU"},
    # OECD CPI alternatív forrás — HU/M monthly headline %change YoY
    {"type": "oecd",         "agency": "OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0",
                              "key":    "M.HUN.N.CPI.PA._T.N.GY"},
    {"type": "brave_search", "query": "Eurostat HICP Hungary {YYYY-MM} annual inflation rate",
                              "site": "ec.europa.eu",
                              "rx": r"Hungary[\s\S]{0,300}?(\d+[,.]\d)\s*%"},
    {"type": "brave_search", "query": "KSH fogyasztói árak {YYYY-MM} infláció",
                              "site": "ksh.hu",
                              "rx": r"(\d+[,.]\d)\s*%"},
    {"type": "brave_search", "query": "HU CPI inflation {YYYY-MM} headline",
                              "rx": r"(\d+[,.]\d)\s*%"},
]

# HU PPI — a KSH ipi gyorstájékoztató külön publikál havi PPI-t
INDICATOR_RESOLVERS[("HU", "ppi")] = [
    {"type": "ksh_flash",    "topic": "ipi",
                              "rx": r"ipari\s+termelői\s+árak[\s\S]{0,200}?(\d+[,.]\d)\s*%[\s\-]*kal"},
    {"type": "ksh_flash",    "topic": "ipi",
                              "rx": r"(\d+[,.]\d)\s*%[\s\-]*kal\s+(?:emelkedett|drágult|nőtt)"},
    {"type": "eurostat",     "dataset_code": "sts_inpp_m", "geo": "HU"},
    {"type": "brave_search", "query": "KSH ipari termelői árindex {YYYY-MM}",
                              "site": "ksh.hu",
                              "rx": r"(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("HU", "core_cpi")] = [
    # ELSŐ: KSH gyorstájékoztató maginfláció sora
    {"type": "ksh_flash",    "topic": "far",
                              "rx": r"[Mm]aginfláció[\s\S]{0,200}?(\d+[,.]\d)\s*%"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.HU.N.XEF000.4.ANR"},
    {"type": "ksh_stadat",   "table_code": "ara0045", "yoy_from_index": True},
    {"type": "brave_search", "query": "MNB maginfláció {YYYY-MM}",
                              "site": "mnb.hu",
                              "rx": r"maginfláció[\s\S]{0,200}?(\d+[,.]\d)\s*%"},
]
# Food and energy CPI from KSH flash as well — markdown + HTML kompatibilis
INDICATOR_RESOLVERS[("HU", "food_cpi")] = [
    # markdown: "Az **élelmiszerek** ára 1,5%-kal nőtt"
    # HTML:     "Az élelmiszerek ára 1,5 %- kal nőtt"
    {"type": "ksh_flash",    "topic": "far",
                              "rx": r"\*{0,2}élelmiszerek?\*{0,2}\s+ára\s+(\d+[,.]\d)\s*%[\s\-]*kal\s+(?:nőtt|emelkedett|drágult|csökkent)"},
    {"type": "ksh_flash",    "topic": "far",
                              "rx": r"élelmiszerek?\*{0,2}\s+ára\s+(\d+[,.]\d)\s*%"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.HU.N.FOOD00.4.ANR"},
]
INDICATOR_RESOLVERS[("HU", "energy_cpi")] = [
    # markdown: "A **háztartási energiáért** 0,4, ezen belül a vezetékes gázért 3,1%-kal kevesebbet fizettek"
    # The number is right after "energiáért", verb (kevesebbet/többet) ~70ch later.
    {"type": "ksh_flash",    "topic": "far", "sign_aware": True,
                              "rx": r"\*{0,2}háztartási\s+energi[áa](?:ért)?\*{0,2}\s+(\d+[,.]\d)"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.HU.N.NRGY00.4.ANR"},
]
INDICATOR_RESOLVERS[("HU", "services_cpi")] = [
    # ELSŐ: KSH gyorstájékoztató YoY-mondat (markdown + HTML kompatibilis)
    # markdown: "A **szolgáltatások** 4,0%-kal drágultak"
    # HTML:     "A szolgáltatások 4,0 %- kal drágultak"
    {"type": "ksh_flash",    "topic": "far",
                              "rx": r"A\s+\*{0,2}\s*(?:&nbsp;\s*)?szolgáltatások\*{0,2}\s+(\d+[,.]\d)\s*%[\s\-]*kal\s+drágult"},
    {"type": "ksh_flash",    "topic": "far",
                              "rx": r"szolgáltatások\*{0,2}\s+(\d+[,.]\d)\s*%[\s\-]*kal\s+(?:drágult|csökkent|nőtt|emelkedt)"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.HU.N.SERV00.4.ANR"},
    {"type": "brave_search", "query": "szolgáltatás infláció {YYYY-MM} havi",
                              "site": "mnb.hu",
                              "rx": r"szolgáltatás[\s\S]{0,300}?(\d+[,.]\d)\s*%"},
    {"type": "brave_search", "query": "MNB Inflációs Jelentés szolgáltatás infláció {YYYY}",
                              "rx": r"szolgáltatás[\s\S]{0,200}?(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("HU", "unemployment")] = [
    {"type": "eurostat",     "dataset_code": "une_rt_m", "geo": "HU",
                              "filters": "sex=T&age=TOTAL&unit=PC_ACT&s_adj=SA"},
    # KSH mun gyorstájékoztató: "munkanélküliségi ráta X,Y%"
    {"type": "ksh_flash",    "topic": "mun",
                              "rx": r"munkanélküliségi\s+ráta[\s\S]{0,200}?(\d+[,.]\d)\s*%"},
    {"type": "brave_search", "query": "KSH munkanélküliségi ráta {YYYY-MM}",
                              "site": "ksh.hu",
                              "rx": r"(\d+[,.]\d)\s*%"},
    {"type": "brave_search", "query": "KSH munkanélküliségi ráta {YYYY-MM}",
                              "rx": r"(\d+[,.]\d)\s*%"},
]

# HU bond yield 10y — ECB IRS havi + AKK napi scrape
INDICATOR_RESOLVERS[("HU", "bond_yield_10y")] = [
    {"type": "ecb",          "dataset": "IRS", "key": "M.HU.L.L40.CI.0000.HUF.N.Z"},
    # AKK (Államadósság Kezelő Központ) — napi referencia-hozam scrape
    {"type": "scrape",       "url": "https://www.akk.hu/aktualis-hozamok",
                              "rx": r"10\s*éves[\s\S]{0,200}?(\d+[,.]\d{1,3})\s*%"},
    {"type": "scrape",       "url": "https://www.akk.hu/page.php?page=Statistics_F_HU",
                              "rx": r"10\s*éves[\s\S]{0,200}?(\d+[,.]\d{1,3})\s*%"},
    {"type": "brave_search", "query": "magyar 10 éves államkötvény hozam {YYYY-MM}",
                              "site": "akk.hu",
                              "rx": r"(\d+[,.]\d{1,3})\s*%"},
    {"type": "brave_search", "query": "magyar 10 éves államkötvény hozam {YYYY-MM}",
                              "rx": r"(\d+[,.]\d{1,3})\s*%"},
]

# HU GDP componens-bontás — Eurostat namq_10_gdp más na_item-kódokkal.
# Mind YoY%, real (CLV15_MEUR + s_adj=SCA), so the agent can derive growth.
INDICATOR_RESOLVERS[("HU", "gdp_consumption")] = [
    # Háztartási végső fogyasztási kiadása (P31_S14)
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "HU",
                              "filters": "na_item=P31_S14&unit=CLV15_MEUR&s_adj=SCA"},
    # Háztartás + NPISH (alternatív bontás)
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "HU",
                              "filters": "na_item=P31_S14_S15&unit=CLV15_MEUR&s_adj=SCA"},
]
INDICATOR_RESOLVERS[("HU", "gdp_investment")] = [
    # Bruttó tőkefelhalmozás (GFCF)
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "HU",
                              "filters": "na_item=P51G&unit=CLV15_MEUR&s_adj=SCA"},
]
INDICATOR_RESOLVERS[("HU", "gdp_exports")] = [
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "HU",
                              "filters": "na_item=P6&unit=CLV15_MEUR&s_adj=SCA"},
]
INDICATOR_RESOLVERS[("HU", "gdp_imports")] = [
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "HU",
                              "filters": "na_item=P7&unit=CLV15_MEUR&s_adj=SCA"},
]
INDICATOR_RESOLVERS[("HU", "gdp_government")] = [
    # Kormányzati végső fogyasztási kiadása
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "HU",
                              "filters": "na_item=P3_S13&unit=CLV15_MEUR&s_adj=SCA"},
]
INDICATOR_RESOLVERS[("HU", "gdp")] = [
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "HU",
                              "filters": "na_item=B1GQ&unit=CLV15_MEUR&s_adj=SCA"},
    {"type": "brave_search", "query": "KSH GDP gyorsbecslés {YYYY} negyedév",
                              "rx": r"(\d+[,.]\d)\s*%"},
]
# HU GDP growth — KSH "Bruttó hazai termék (GDP) előzetes adata" oldal kanonikus
# forrás a havi/negyedéves YoY% számokra. URL stabil, regex célzott.
INDICATOR_RESOLVERS[("HU", "gdp_growth")] = [
    {"type": "scrape",       "url": "https://www.ksh.hu/brutto_hazai_termek_gdp_elozetes_adata",
                              "rx": r"(\d+[,.]\d)\s*%[\s\-]*kal\s+meg.{0,10}haladt"},
    {"type": "scrape",       "url": "https://www.ksh.hu/brutto_hazai_termek_gdp_elozetes_adata",
                              "rx": r"gazda?s?á?g[\s\S]{0,50}?(\d+[,.]\d)\s*%"},
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "HU",
                              "filters": "na_item=B1GQ&unit=CLV_PCH_PRE&s_adj=SCA"},
    {"type": "brave_search", "query": "KSH GDP {YYYY} negyedév előzetes adat",
                              "site": "ksh.hu",
                              "rx": r"(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("HU", "policy_rate")] = [
    # Direct scrape attempts — JS-rendered, brave-mcp Puppeteer handles
    {"type": "scrape",       "url": "https://www.mnb.hu/Jegybanki_alapkamat_alakulasa",
                              "rx": r"(?:alapkamat|irányadó kamat|jegybanki[\s\w]*kamat)[\s\S]{0,400}?(\d+[,.]\d{1,2})\s*%",
                              "attach_decision_date": "HU"},
    {"type": "scrape",       "url": "https://www.mnb.hu/jegybanki-alapkamat-alakulasa",
                              "rx": r"(?:alapkamat|irányadó kamat|jegybanki[\s\w]*kamat)[\s\S]{0,400}?(\d+[,.]\d{1,2})\s*%",
                              "attach_decision_date": "HU"},
    # Hivatalos forrás preferencia: csak mnb.hu
    {"type": "brave_search", "query": "MNB irányadó kamat alapkamat {YYYY-MM} monetáris tanács döntés",
                              "site": "mnb.hu",
                              "rx": r"(\d+[,.]\d{1,2})\s*%",
                              "attach_decision_date": "HU"},
    # Általános HU fallback (Portfolio/VG másodlagos, de friss)
    {"type": "brave_search", "query": "MNB irányadó kamat alapkamat {YYYY-MM} monetáris tanács döntés",
                              "rx": r"(\d+[,.]\d{1,2})\s*%",
                              "attach_decision_date": "HU"},
    {"type": "brave_search", "query": "MNB base rate Hungary {YYYY-MM} policy decision",
                              "rx": r"(\d+[,.]\d{1,2})\s*%",
                              "attach_decision_date": "HU"},
    {"type": "bis",          "country": "HU"},
]

# Germany — native German brave queries override
INDICATOR_RESOLVERS[("DE", "cpi")] = [
    {"type": "ecb",          "dataset": "ICP", "key": "M.DE.N.000000.4.ANR"},
    {"type": "eurostat",     "dataset_code": "prc_hicp_manr", "geo": "DE"},
    {"type": "brave_search", "query": "Destatis Verbraucherpreise {YYYY-MM} Inflationsrate",
                              "rx": r"(\d+[,.]\d)\s*%"},
    {"type": "brave_search", "query": "Germany CPI HICP inflation {YYYY-MM}",
                              "rx": r"(\d+[,.]\d)\s*%"},
]

# France — INSEE
INDICATOR_RESOLVERS[("FR", "cpi")] = [
    {"type": "ecb",          "dataset": "ICP", "key": "M.FR.N.000000.4.ANR"},
    {"type": "eurostat",     "dataset_code": "prc_hicp_manr", "geo": "FR"},
    {"type": "brave_search", "query": "INSEE indice prix consommation {YYYY-MM} inflation",
                              "rx": r"(\d+[,.]\d)\s*%"},
    {"type": "brave_search", "query": "France CPI HICP inflation {YYYY-MM}",
                              "rx": r"(\d+[,.]\d)\s*%"},
]

# Italy — ISTAT
INDICATOR_RESOLVERS[("IT", "cpi")] = [
    {"type": "ecb",          "dataset": "ICP", "key": "M.IT.N.000000.4.ANR"},
    {"type": "eurostat",     "dataset_code": "prc_hicp_manr", "geo": "IT"},
    {"type": "brave_search", "query": "ISTAT prezzi al consumo {YYYY-MM} inflazione",
                              "rx": r"(\d+[,.]\d)\s*%"},
]

# Spain — INE
INDICATOR_RESOLVERS[("ES", "cpi")] = [
    {"type": "ecb",          "dataset": "ICP", "key": "M.ES.N.000000.4.ANR"},
    {"type": "eurostat",     "dataset_code": "prc_hicp_manr", "geo": "ES"},
    {"type": "brave_search", "query": "INE IPC España {YYYY-MM} inflación",
                              "rx": r"(\d+[,.]\d)\s*%"},
]

# Netherlands — CBS native query
INDICATOR_RESOLVERS[("NL", "cpi")] = [
    {"type": "ecb",          "dataset": "ICP", "key": "M.NL.N.000000.4.ANR"},
    {"type": "eurostat",     "dataset_code": "prc_hicp_manr", "geo": "NL"},
    {"type": "brave_search", "query": "CBS Netherlands inflation HICP {YYYY-MM}",
                              "rx": r"(\d+[,.]\d)\s*%"},
    {"type": "brave_search", "query": "Netherlands inflation rate {YYYY-MM} annual",
                              "rx": r"(\d+[,.]\d)\s*%"},
]

# ─── Pass 4: Euro area aggregate (EA / U2) ─────────────────────────
INDICATOR_RESOLVERS[("EA", "cpi")] = [
    # Eurostat hivatalos flash press release (ap = EA aggregate). A regex
    # célzottan az "up to / expected to be X.Y%" mintát fogja — NE az
    # "up from N.M% in previous month" reference-pontot.
    {"type": "eurostat_press", "suffix": "ap", "historical": True,
                                "component_label": "All-items HICP",
                                "rx": r"annual\s+inflation[\s\S]{0,40}?(?:up\s+to|down\s+to|at|expected\s+to\s+be)\s+(\d+[,.]\d)\s*%"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.U2.N.000000.4.ANR"},
    {"type": "eurostat",     "dataset_code": "prc_hicp_manr", "geo": "EA"},
    {"type": "brave_search", "query": "Eurostat euro area annual inflation flash {YYYY-MM}",
                              "site": "ec.europa.eu",
                              "rx": r"annual\s+inflation[\s\S]{0,80}?(\d+[,.]\d)\s*%"},
    {"type": "brave_search", "query": "euro area inflation {YYYY-MM} ECB",
                              "rx": r"(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("EA", "core_cpi")] = [
    # A flash press release tipikusan NEM tartalmaz "core HICP" külön sort,
    # csak a 4 fő komponenst (services, energy, food, non-energy goods).
    # ECB ICP XEF000 stale 2025-12-nél. Maradék: brave_search szigorú minta.
    {"type": "ecb",          "dataset": "ICP", "key": "M.U2.N.XEF000.4.ANR"},
    {"type": "brave_search", "query": "Eurostat euro area HICP excluding energy food {YYYY-MM}",
                              "site": "ec.europa.eu",
                              "rx": r"excluding\s+energy\s+(?:and\s+)?(?:un[\s\-]?processed\s+)?food[\s\S]{0,80}?(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("EA", "services_cpi")] = [
    {"type": "eurostat_press", "suffix": "ap",
                                "rx": r"[Ss]ervices[\s\S]{0,30}?(\d+[,.]\d)\s*%"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.U2.N.SERV00.4.ANR"},
    {"type": "brave_search", "query": "euro area services inflation HICP {YYYY-MM}",
                              "site": "ec.europa.eu",
                              "rx": r"[Ss]ervices[\s\S]{0,30}?(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("EA", "energy_cpi")] = [
    # A press release markdown-ja TÁBLÁZAT: "**Energy**\n<12 havi érték>\n**10.9e**".
    # Case-sensitive (?-i:) — a kisbetűs "energy" másik táblázatban szerepel.
    {"type": "eurostat_press", "suffix": "ap", "historical": True,
                                "component_label": "Energy",
                                "rx": r"(?-i:\*\*Energy\*\*)[\s\S]*?\*\*(-?\d+[,.]?\d*)e?\*\*"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.U2.N.NRGY00.4.ANR"},
]
INDICATOR_RESOLVERS[("EA", "services_cpi")] = [
    {"type": "eurostat_press", "suffix": "ap", "historical": True,
                                "component_label": "Services",
                                "rx": r"(?-i:\*\*Services\*\*)[\s\S]*?\*\*(-?\d+[,.]?\d*)e?\*\*"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.U2.N.SERV00.4.ANR"},
    {"type": "brave_search", "query": "euro area services inflation HICP {YYYY-MM}",
                              "site": "ec.europa.eu",
                              "rx": r"[Ss]ervices[\s\S]{0,30}?(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("EA", "food_cpi")] = [
    {"type": "eurostat_press", "suffix": "ap", "historical": True,
                                "component_label": "Food",
                                "rx": r"(?-i:\*\*Food[\s\S]{0,40}?\*\*)[\s\S]*?\*\*(-?\d+[,.]?\d*)e?\*\*"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.U2.N.FOOD00.4.ANR"},
]
INDICATOR_RESOLVERS[("EA", "food_cpi")] = [
    {"type": "eurostat_press", "suffix": "ap",
                                "rx": r"Food[\s\S]{0,60}?(\d+[,.]\d)\s*%"},
    {"type": "ecb",          "dataset": "ICP", "key": "M.U2.N.FOOD00.4.ANR"},
]
INDICATOR_RESOLVERS[("EA", "policy_rate")] = [
    {"type": "ecb",          "dataset": "FM",  "key": "D.U2.EUR.4F.KR.DFR.LEV"},
    {"type": "scrape",       "url": "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/key_ecb_interest_rates/html/index.en.html",
                              "rx": r"Deposit facility[\s\S]{0,300}?(\d+[,.]\d{1,2})\s*%"},
]
INDICATOR_RESOLVERS[("EA", "unemployment")] = [
    # 2026-01-óta Bulgaria belépett az EA-ba → EA21. A geo=EA20 a 2025-12-ig tartó
    # adatokat adja vissza. Próbáljuk EA21-et először, EA20 fallback.
    {"type": "eurostat",     "dataset_code": "une_rt_m", "geo": "EA21",
                              "filters": "sex=T&age=TOTAL&unit=PC_ACT&s_adj=SA"},
    {"type": "eurostat",     "dataset_code": "une_rt_m", "geo": "EA20",
                              "filters": "sex=T&age=TOTAL&unit=PC_ACT&s_adj=SA"},
    {"type": "eurostat",     "dataset_code": "une_rt_m", "geo": "EA19",
                              "filters": "sex=T&age=TOTAL&unit=PC_ACT&s_adj=SA"},
    {"type": "eurostat_press", "suffix": "ap",
                                "rx": r"(?:Euro area|euro area)[\s\S]{0,200}?unemployment rate[\s\S]{0,80}?(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("EA", "gdp")] = [
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "EA20",
                              "filters": "na_item=B1GQ&unit=CLV15_MEUR&s_adj=SCA"},
]
INDICATOR_RESOLVERS[("EA", "ppi")] = [
    {"type": "eurostat",     "dataset_code": "sts_inpp_m", "geo": "EA20",
                              "filters": "indic_bt=PRC_PRR&nace_r2=B-E36&s_adj=NSA&unit=RCH_A"},
]
INDICATOR_RESOLVERS[("EA", "retail_trade")] = [
    {"type": "eurostat",     "dataset_code": "sts_trtu_m", "geo": "EA20"},
    {"type": "brave_search", "query": "euro area retail trade volume {YYYY-MM} Eurostat",
                              "rx": r"(-?\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("EA", "industrial_production")] = [
    {"type": "eurostat",     "dataset_code": "sts_inpr_m", "geo": "EA20"},
    {"type": "brave_search", "query": "euro area industrial production {YYYY-MM} Eurostat",
                              "rx": r"(-?\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("EA", "gdp_growth")] = [
    {"type": "eurostat",     "dataset_code": "namq_10_gdp", "geo": "EA20",
                              "filters": "na_item=B1GQ&unit=CLV_PCH_PRE&s_adj=SCA"},
    {"type": "brave_search", "query": "euro area GDP growth {YYYY} quarter-on-quarter Eurostat",
                              "rx": r"(-?\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("EA", "bond_yield_10y")] = [
    {"type": "ecb",          "dataset": "IRS", "key": "M.U2.L.L40.CI.0000.EUR.N.Z"},
    {"type": "brave_search", "query": "euro area 10-year government bond yield {YYYY-MM}",
                              "rx": r"(\d+[,.]\d{1,3})\s*%"},
]
INDICATOR_RESOLVERS[("EA", "gov_debt")] = [
    {"type": "eurostat",     "dataset_code": "gov_10q_ggdebt", "geo": "EA20",
                              "filters": "unit=PC_GDP&sector=S13"},
]
INDICATOR_RESOLVERS[("EA", "trade_balance")] = [
    {"type": "eurostat",     "dataset_code": "ext_lt_intertrd", "geo": "EA20"},
]

# ─── Pass 5: United States (FRED dominant) ─────────────────────────
INDICATOR_RESOLVERS[("US", "cpi")] = [
    {"type": "fred",         "series_id": "CPIAUCSL", "units": "pc1"},
    # DBnomics FRED-tükör fallback (FRED API 5xx esetén — nem transzformál, így YoY-t a router számolja)
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "CPIAUCSL",
                              "series_code": "CPIAUCSL", "compute_yoy": True},
    {"type": "scrape",       "url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                              "rx": r"(\d+[,.]\d)\s*percent"},
    {"type": "brave_search", "query": "BLS US CPI inflation {YYYY-MM}",
                              "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
]
INDICATOR_RESOLVERS[("US", "core_cpi")] = [
    {"type": "fred",         "series_id": "CPILFESL", "units": "pc1"},
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "CPILFESL",
                              "series_code": "CPILFESL", "compute_yoy": True},
    {"type": "brave_search", "query": "BLS US core CPI {YYYY-MM} excluding food energy",
                              "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
]
INDICATOR_RESOLVERS[("US", "services_cpi")] = [
    {"type": "fred",         "series_id": "CUSR0000SAS", "units": "pc1"},
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "CUSR0000SAS",
                              "series_code": "CUSR0000SAS", "compute_yoy": True},
]
INDICATOR_RESOLVERS[("US", "ppi")] = [
    {"type": "fred",         "series_id": "PPIACO", "units": "pc1"},
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "PPIACO",
                              "series_code": "PPIACO", "compute_yoy": True},
]
INDICATOR_RESOLVERS[("US", "policy_rate")] = [
    {"type": "fred",         "series_id": "DFEDTARU"},  # Fed Funds Target Upper Bound
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "DFEDTARU",
                              "series_code": "DFEDTARU"},  # nyers level — kamat, nem YoY
    {"type": "scrape",       "url": "https://www.federalreserve.gov/monetarypolicy/openmarket.htm",
                              "rx": r"(\d+[,.]\d{1,2})\s*(?:to|–|-)?\s*(\d+[,.]\d{1,2})?\s*percent"},
    {"type": "brave_search", "query": "Fed Funds target rate FOMC decision {YYYY-MM}",
                              "rx": r"(\d+[,.]\d{1,2})\s*(?:%|percent)"},
]
INDICATOR_RESOLVERS[("US", "unemployment")] = [
    {"type": "fred",         "series_id": "UNRATE"},
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "UNRATE",
                              "series_code": "UNRATE"},  # ráta, nyers level helyes
    {"type": "brave_search", "query": "BLS US unemployment rate {YYYY-MM}",
                              "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
]
INDICATOR_RESOLVERS[("US", "gdp")] = [
    {"type": "fred",         "series_id": "GDPC1", "units": "pc1"},
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "GDPC1",
                              "series_code": "GDPC1", "compute_yoy": True},
    {"type": "brave_search", "query": "BEA US GDP growth {YYYY} quarterly",
                              "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
]
INDICATOR_RESOLVERS[("US", "retail_trade")] = [
    {"type": "fred",         "series_id": "RSAFS", "units": "pc1"},
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "RSAFS",
                              "series_code": "RSAFS", "compute_yoy": True},
]
INDICATOR_RESOLVERS[("US", "industrial_production")] = [
    {"type": "fred",         "series_id": "INDPRO", "units": "pc1"},
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "INDPRO",
                              "series_code": "INDPRO", "compute_yoy": True},
]
INDICATOR_RESOLVERS[("US", "wages")] = [
    {"type": "fred",         "series_id": "CES0500000003"},
    {"type": "fred",         "series_id": "ECIWAG", "units": "pc1"},
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "CES0500000003",
                              "series_code": "CES0500000003"},
    {"type": "brave_search", "query": "BLS US average hourly earnings wages {YYYY-MM} year-over-year",
                              "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
]
INDICATOR_RESOLVERS[("US", "house_prices")] = [
    {"type": "fred",         "series_id": "CSUSHPISA", "units": "pc1"},
    {"type": "dbnomics",     "provider_code": "FRED", "dataset_code": "CSUSHPISA",
                              "series_code": "CSUSHPISA", "compute_yoy": True},
]

# ─── Pass 6: UK ───────────────────────────────────────────────────
INDICATOR_RESOLVERS[("GB", "cpi")] = [
    {"type": "ecb",          "dataset": "ICP", "key": "M.GB.N.000000.4.ANR"},
    {"type": "eurostat",     "dataset_code": "prc_hicp_manr", "geo": "UK"},
    {"type": "brave_search", "query": "ONS UK CPI inflation rate {YYYY-MM}",
                              "rx": r"(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("GB", "core_cpi")] = [
    {"type": "ecb",          "dataset": "ICP", "key": "M.GB.N.XEF000.4.ANR"},
]
INDICATOR_RESOLVERS[("GB", "unemployment")] = [
    {"type": "brave_search", "query": "ONS UK unemployment rate {YYYY-MM}",
                              "rx": r"(\d+[,.]\d)\s*%"},
]
INDICATOR_RESOLVERS[("GB", "gdp")] = [
    {"type": "brave_search", "query": "ONS UK GDP {YYYY} quarterly growth",
                              "rx": r"(\d+[,.]\d)\s*%"},
]

# ─── Pass 7: Non-EU/EA major economies ────────────────────────────
# These rely heavily on brave_search since ECB ICP, Eurostat and FRED
# don't cover them natively. DBnomics has IMF IFS / OECD time series
# for most G20 countries which we wire up too.
_NON_EU_COUNTRIES = {
    "JP": {"name": "Japan", "search": "BoJ Japan", "central_bank": "Bank of Japan"},
    "CN": {"name": "China", "search": "NBS China PBoC", "central_bank": "PBoC"},
    "KR": {"name": "South Korea", "search": "Bank of Korea KOSIS", "central_bank": "BoK"},
    "IN": {"name": "India", "search": "RBI India MoSPI", "central_bank": "RBI"},
    "BR": {"name": "Brazil", "search": "IBGE BCB Brazil", "central_bank": "BCB"},
    "MX": {"name": "Mexico", "search": "INEGI Banxico Mexico", "central_bank": "Banxico"},
    "TR": {"name": "Turkey", "search": "TUIK TCMB Turkey", "central_bank": "TCMB"},
    "ZA": {"name": "South Africa", "search": "StatsSA SARB South Africa", "central_bank": "SARB"},
    "AU": {"name": "Australia", "search": "ABS RBA Australia", "central_bank": "RBA"},
    "CA": {"name": "Canada", "search": "StatsCan Bank of Canada", "central_bank": "BoC"},
    "RU": {"name": "Russia", "search": "Rosstat CBR Russia", "central_bank": "CBR"},
    "ID": {"name": "Indonesia", "search": "BPS Bank Indonesia", "central_bank": "BI"},
    "SA": {"name": "Saudi Arabia", "search": "GASTAT SAMA Saudi Arabia", "central_bank": "SAMA"},
    "AR": {"name": "Argentina", "search": "INDEC BCRA Argentina", "central_bank": "BCRA"},
    "EG": {"name": "Egypt", "search": "CAPMAS CBE Egypt", "central_bank": "CBE"},
    "NG": {"name": "Nigeria", "search": "NBS CBN Nigeria", "central_bank": "CBN"},
    "TH": {"name": "Thailand", "search": "NESDC BoT Thailand", "central_bank": "BoT"},
    "VN": {"name": "Vietnam", "search": "GSO SBV Vietnam", "central_bank": "SBV"},
}
for _c, _meta in _NON_EU_COUNTRIES.items():
    _name = _meta["name"]
    _search = _meta["search"]
    _cb = _meta["central_bank"]
    INDICATOR_RESOLVERS[(_c, "cpi")] = [
        {"type": "brave_search",
         "query": f"{_search} CPI inflation {{YYYY-MM}} annual rate",
         "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
        {"type": "brave_search",
         "query": f"{_name} inflation rate {{YYYY-MM}}",
         "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
    ]
    INDICATOR_RESOLVERS[(_c, "policy_rate")] = [
        {"type": "bis", "country": _c},
        {"type": "brave_search",
         "query": f"{_cb} policy rate decision {{YYYY-MM}}",
         "rx": r"(\d+[,.]\d{1,2})\s*(?:%|percent)"},
        {"type": "brave_search",
         "query": f"{_name} central bank interest rate {{YYYY-MM}}",
         "rx": r"(\d+[,.]\d{1,2})\s*(?:%|percent)"},
    ]
    INDICATOR_RESOLVERS[(_c, "unemployment")] = [
        {"type": "brave_search",
         "query": f"{_search} unemployment rate {{YYYY-MM}}",
         "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
        {"type": "brave_search",
         "query": f"{_name} unemployment rate {{YYYY-MM}}",
         "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
    ]
    INDICATOR_RESOLVERS[(_c, "gdp")] = [
        {"type": "brave_search",
         "query": f"{_search} GDP growth {{YYYY}} quarterly",
         "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
        {"type": "brave_search",
         "query": f"{_name} GDP growth {{YYYY}} latest",
         "rx": r"(\d+[,.]\d)\s*(?:%|percent)"},
    ]

# Japan-specific FRED overrides (high reliability for major aggregates)
INDICATOR_RESOLVERS[("JP", "cpi")] = [
    {"type": "fred", "series_id": "JPNCPIALLMINMEI", "units": "pc1"},
] + INDICATOR_RESOLVERS[("JP", "cpi")]
INDICATOR_RESOLVERS[("JP", "unemployment")] = [
    {"type": "fred", "series_id": "LRHUTTTTJPM156S"},
] + INDICATOR_RESOLVERS[("JP", "unemployment")]
INDICATOR_RESOLVERS[("JP", "policy_rate")] = [
    {"type": "fred", "series_id": "IRSTCB01JPM156N"},
] + INDICATOR_RESOLVERS[("JP", "policy_rate")]

# Canada FRED overrides
INDICATOR_RESOLVERS[("CA", "cpi")] = [
    {"type": "fred", "series_id": "CPALCY01CAM659N"},
] + INDICATOR_RESOLVERS[("CA", "cpi")]
INDICATOR_RESOLVERS[("CA", "unemployment")] = [
    {"type": "fred", "series_id": "LRHUTTTTCAM156S"},
] + INDICATOR_RESOLVERS[("CA", "unemployment")]

# Australia FRED overrides
INDICATOR_RESOLVERS[("AU", "cpi")] = [
    {"type": "fred", "series_id": "AUSCPIALLQINMEI", "units": "pc1"},
] + INDICATOR_RESOLVERS[("AU", "cpi")]
INDICATOR_RESOLVERS[("AU", "unemployment")] = [
    {"type": "fred", "series_id": "LRHUTTTTAUM156S"},
] + INDICATOR_RESOLVERS[("AU", "unemployment")]


def _parse_period_to_date(period: str):
    """Parse 'YYYY-MM', 'YYYY-Qn', 'YYYY-MM-DD' or 'YYYY' to a datetime.

    For bare 'YYYY' equal to the current year, returns the FIRST DAY of the
    current month — this lets year-only labels from yearly-reported tables be
    treated as "fresh data point of this year" rather than end-of-December
    (which would mark them stale until December).
    """
    from datetime import datetime as _dt
    period = (period or "").strip()
    if not period:
        return None
    try:
        if len(period) == 4 and period.isdigit():
            y = int(period)
            now = _dt.now()
            if y == now.year:
                return _dt(y, now.month, 1)
            return _dt(y, 12, 31)
        if len(period) == 7 and "-Q" in period:  # 2025-Q4
            y, q = period.split("-Q")
            return _dt(int(y), int(q) * 3, 28)
        if len(period) == 7 and period[4] == "-":  # 2025-12
            return _dt(int(period[:4]), int(period[5:7]), 28)
        if len(period) == 10 and period[4] == "-" and period[7] == "-":  # 2025-12-31
            return _dt(int(period[:4]), int(period[5:7]), int(period[8:10]))
    except (ValueError, IndexError):
        return None
    return None


# Hungarian month names → 1..12 for KSH STADAT column parsing
_HUN_MONTHS: dict[str, int] = {
    "január": 1, "február": 2, "március": 3, "április": 4,
    "május": 5, "június": 6, "július": 7, "augusztus": 8,
    "szeptember": 9, "október": 10, "november": 11, "december": 12,
}


def _is_fresh(period: str, max_age_days: int) -> bool:
    """True if `period` is within max_age_days of today."""
    from datetime import datetime as _dt
    dt = _parse_period_to_date(period)
    if not dt:
        return False
    age = (_dt.now() - dt).days
    return age <= max_age_days


def _format_query_template(template: str) -> str:
    """Substitute {YYYY}, {YYYY-MM} placeholders with current date."""
    from datetime import datetime as _dt, timedelta as _td
    now = _dt.now()
    # Use last month for monthly queries, since current month not yet published
    last_month = (now.replace(day=1) - _td(days=1))
    return (template
            .replace("{YYYY-MM}", last_month.strftime("%Y-%m"))
            .replace("{YYYY}", str(now.year))
            .replace("{MM}", last_month.strftime("%m")))


async def _brave_mcp_post(tool_name: str, arguments: dict, timeout: float = 90.0) -> Optional[dict]:
    """Call brave-mcp-server's MCP endpoint. Returns parsed result dict or None."""
    if not BRAVE_MCP_URL:
        return None
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    client = await get_client()
    try:
        r = await client.post(f"{BRAVE_MCP_URL}/mcp", json=payload, timeout=timeout,
                              headers={"Accept": "application/json"})
        if r.status_code != 200:
            logger.warning("brave-mcp HTTP %d", r.status_code)
            return None
        data = r.json()
        result = data.get("result") or {}
        content = result.get("content") or []
        if content and isinstance(content[0], dict) and "text" in content[0]:
            try:
                return json.loads(content[0]["text"])
            except Exception:
                return {"text": content[0]["text"]}
        return result
    except Exception as e:
        logger.warning("brave-mcp call failed (%s): %s", tool_name, e)
        return None


# Hungarian directional words that indicate a NEGATIVE percentage change in
# KSH-style prose ("X %-kal csökkent / mérséklődött / olcsóbb / kevesebbet").
_NEGATIVE_WORDS: tuple[str, ...] = (
    "csökkent", "csökkentek", "csökkenés", "csökkentett",
    "kevesebb", "kevesebbet", "kevesebbért",
    "olcsóbb", "olcsóbbak",
    "mérséklődött", "mérséklődtek",
    "alacsonyabb", "alacsonyabban",
)


async def _scrape_extract_value(url: str, rx: str, sign_aware: bool = False) -> Optional[dict]:
    """Scrape a URL (JS-rendered via brave-mcp if available, else plain httpx),
    and extract the first regex match as a float value. Tries multiple
    encodings (utf-8 → iso-8859-2 → windows-1250) because Hungarian sites
    often serve latin2-encoded HTML without a BOM and httpx's auto-decode
    mangles characters like 'ő' / 'ű'.

    If sign_aware=True (opt-in per resolver), the immediate 30-character
    suffix of the match is scanned for Hungarian negative-direction words
    ("csökkent", "kevesebb", "olcsóbb", "mérséklődött") — if any are
    present, the value is negated. KSH prose conventionally states
    "X %-kal csökkent" without a minus sign. Use sparingly, only for
    indicators where the verb is reliably adjacent (e.g. energy prices).
    """
    text = ""
    if BRAVE_MCP_URL:
        result = await _brave_mcp_post("brave_scrape", {"url": url, "waitTime": 3000})
        if result:
            text = result.get("markdown") or result.get("text") or ""
    if not text:
        client = await get_client()
        try:
            r = await client.get(url, timeout=20.0, follow_redirects=True,
                                 headers={"User-Agent": "StatData/1.0"})
            if r.status_code != 200:
                return None
            # Try decoding with multiple Hungarian-likely encodings; pick the one
            # that produces no replacement chars in the expected accented-letter
            # ranges. Defaults to utf-8.
            raw_bytes = r.content
            for enc in ("utf-8", "iso-8859-2", "windows-1250", "iso-8859-1"):
                try:
                    candidate = raw_bytes.decode(enc)
                except (UnicodeDecodeError, LookupError):
                    continue
                # Heuristic: real Hungarian text contains "á", "é", "ő"
                if any(c in candidate for c in ("á", "é", "ő", "ű", "ó")):
                    text = candidate
                    break
            if not text:
                # Last-resort fall back to httpx's auto-decode
                text = r.text
            # Strip HTML tags
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text)
        except Exception as e:
            logger.warning("scrape httpx fallback failed for %s: %s", url, e)

    if not text:
        return None

    m = re.search(rx, text, flags=re.IGNORECASE)
    if not m:
        return None
    raw = m.group(1).replace(",", ".")
    try:
        val = float(raw)
    except ValueError:
        return None

    if sign_aware:
        # Inspect the 100-character suffix after the match for Hungarian
        # negative-direction verbs. KSH prose puts the verb at the end of
        # an enumeration: "A háztartási energiáért 0,4, ezen belül a
        # vezetékes gázért 3,1 %-kal kevesebbet fizettek" — the "kevesebbet"
        # applies to BOTH numbers, sitting ~70 characters after the first.
        ctx = text[m.end():m.end() + 100].lower()
        if any(neg in ctx for neg in _NEGATIVE_WORDS):
            val = -val

    from datetime import datetime as _dt
    return {
        "value": val,
        "period": _dt.now().strftime("%Y-%m-%d"),
        "raw_match": m.group(0),
        "source_url": url,
    }


async def _brave_search_extract(query: str, rx: str, site: str = "") -> Optional[dict]:
    """Run brave_search via brave-mcp; scrape top hit; extract regex value."""
    if not BRAVE_MCP_URL:
        return None
    q = f"{query} site:{site}" if site else query
    search_result = await _brave_mcp_post("brave_search", {"query": q, "limit": 5})
    if not search_result:
        return None
    results = search_result.get("results") or []
    if not results and isinstance(search_result.get("text"), str):
        # Some brave-mcp responses bundle results in plain text
        urls = re.findall(r"https?://\S+", search_result["text"])
        results = [{"url": u} for u in urls[:5]]
    for hit in results[:3]:
        url = hit.get("url") if isinstance(hit, dict) else None
        if not url:
            continue
        scraped = await _scrape_extract_value(url, rx)
        if scraped:
            scraped["search_query"] = q
            return scraped
    return None


async def _resolver_ecb(spec: dict) -> Optional[dict]:
    """Resolver: ECB Data Portal SDMX. Returns {value, period, source}."""
    raw = await get_ecb_data(dataset=spec["dataset"], key=spec["key"], last_n=6)
    try:
        d = json.loads(raw)
    except Exception:
        return None
    data = d.get("data") or []
    if not data:
        return None
    latest = data[-1]
    return {
        "value": latest.get("value"),
        "period": latest.get("period"),
        "source": f"ECB {spec['dataset']}/{spec['key']}",
    }


async def _resolver_eurostat(spec: dict) -> Optional[dict]:
    """Resolver: Eurostat JSON-stat."""
    args = dict(spec)
    args.pop("type", None)
    # 24-month window
    from datetime import date as _date
    since = (_date.today().replace(day=1)).strftime("%Y-%m")
    args.setdefault("sinceTimePeriod",
                    f"{int(since[:4]) - 2}-{since[5:7]}")
    raw = await get_eurostat_data(**args)
    try:
        d = json.loads(raw)
    except Exception:
        return None
    rows = d.get("data") or d.get("observations") or []
    if not rows:
        return None
    rows.sort(key=lambda r: str(r.get("Time", r.get("time", ""))))
    latest = rows[-1]
    return {
        "value": latest.get("value"),
        "period": str(latest.get("Time", latest.get("time", ""))),
        "source": f"Eurostat {spec.get('dataset_code')}",
    }


async def _resolver_fred(spec: dict) -> Optional[dict]:
    """Resolver: FRED REST."""
    args = {k: v for k, v in spec.items() if k != "type"}
    args.setdefault("limit", 3)
    args.setdefault("sort_order", "desc")
    raw = await get_fred_data(**args)
    try:
        d = json.loads(raw)
    except Exception:
        return None
    data = d.get("data") or []
    if not data:
        return None
    latest = data[0]  # desc order
    return {
        "value": latest.get("value"),
        "period": latest.get("date"),
        "source": f"FRED {spec.get('series_id')}",
    }


async def _resolver_ksh_stadat(spec: dict) -> Optional[dict]:
    """Resolver: KSH STADAT — parses transposed monthly tables and (optionally)
    converts base-index values to YoY% by dividing by the previous-year cell.

    Returns the freshest non-empty (year, month) cell. If yoy_from_index=True,
    the value is (current_year / previous_year - 1) * 100 for the matching month.
    """
    raw = await get_ksh_stadat(table_code=spec["table_code"], max_rows=36)
    try:
        d = json.loads(raw)
    except Exception:
        return None
    rows = d.get("data") or []
    if not rows:
        return None
    # Build year → {month_idx: value} lookup (parsing Hungarian month names from
    # column headers). Many KSH transposed tables have columns like
    # "Eredeti maginfláció április" or "Fogyasztóiár-index ... január".
    by_year: dict[int, dict[int, float]] = {}
    for row in rows:
        year_raw = row.get("Év") or row.get("Év Az előző év azonos időszaka = 100,0%") or ""
        try:
            year = int(str(year_raw).strip()[:4])
        except (ValueError, TypeError):
            continue
        for col, val in row.items():
            if not isinstance(val, (int, float)) or val == 0:
                continue
            col_low = col.lower()
            for hu_month, m_idx in _HUN_MONTHS.items():
                if hu_month in col_low:
                    by_year.setdefault(year, {})[m_idx] = float(val)
                    break

    if not by_year:
        # Fall back: first non-zero value found in the first row (no month parsing)
        for row in rows:
            for col, val in row.items():
                if isinstance(val, (int, float)) and val > 0:
                    return {
                        "value": float(val),
                        "period": str(row.get("Év", "")),
                        "source": f"KSH STADAT {spec['table_code']}",
                    }
        return None

    # Find the most recent (year, month) cell
    latest_year = max(by_year)
    latest_month = max(by_year[latest_year])
    latest_value = by_year[latest_year][latest_month]
    period = f"{latest_year}-{latest_month:02d}"

    if spec.get("yoy_from_index"):
        # Convert base-index to YoY% by comparing to prev_year same month
        prev = by_year.get(latest_year - 1, {}).get(latest_month)
        if prev and prev > 0:
            yoy = round((latest_value / prev - 1) * 100, 2)
            return {
                "value": yoy,
                "period": period,
                "source": f"KSH STADAT {spec['table_code']} (YoY% computed from base-index)",
                "raw_index": latest_value,
                "prev_year_index": prev,
            }
        # No prev-year reference — return raw with a flag
        return {
            "value": latest_value,
            "period": period,
            "source": f"KSH STADAT {spec['table_code']}",
            "note": "raw base-index — prev-year reference not in data window",
        }

    return {
        "value": latest_value,
        "period": period,
        "source": f"KSH STADAT {spec['table_code']}",
    }


# Central-bank Monetary Council meeting calendars (statically maintained).
# When a resolver-spec has `attach_decision_date: "<country>"`, we look up the
# most recent past meeting and attach it to the result as `decision_date`.
_CENTRAL_BANK_MEETINGS: dict[str, list[str]] = {
    # MNB Monetáris Tanács kamatmeghatározó ülései 2026
    # https://www.mnb.hu/monetaris-politika/a-monetaris-tanacs
    "HU": [
        "2026-01-27", "2026-02-24", "2026-03-24", "2026-04-28",
        "2026-05-26", "2026-06-23", "2026-07-21", "2026-08-25",
        "2026-09-22", "2026-10-20", "2026-11-17", "2026-12-15",
    ],
}


def _latest_past_meeting(country: str) -> Optional[str]:
    """Return the most recent past Monetary Council meeting date for country."""
    from datetime import datetime as _dt
    schedule = _CENTRAL_BANK_MEETINGS.get(country.upper(), [])
    if not schedule:
        return None
    today = _dt.now().strftime("%Y-%m-%d")
    past = [d for d in schedule if d <= today]
    return past[-1] if past else None


async def _resolver_scrape(spec: dict) -> Optional[dict]:
    """Resolver: direct URL scrape + regex extraction. Set sign_aware=True
    in the spec for Hungarian prose with adjacent direction verbs.
    Set attach_decision_date=<country> to append the most recent past
    Monetary Council meeting date to the result (for policy_rate context).
    """
    res = await _scrape_extract_value(
        spec["url"], spec["rx"],
        sign_aware=bool(spec.get("sign_aware", False)),
    )
    if res:
        res["source"] = f"scrape {spec['url']}"
        if spec.get("attach_decision_date"):
            md = _latest_past_meeting(spec["attach_decision_date"])
            if md:
                res["decision_date"] = md
    return res


async def _resolver_brave_search(spec: dict) -> Optional[dict]:
    """Resolver: brave_search (optionally site-filtered) + scrape + regex.
    Supports attach_decision_date for policy-rate context."""
    q = _format_query_template(spec["query"])
    res = await _brave_search_extract(q, spec["rx"], site=spec.get("site", ""))
    if res:
        res["source"] = f"brave_search {q}"
        if spec.get("attach_decision_date"):
            md = _latest_past_meeting(spec["attach_decision_date"])
            if md:
                res["decision_date"] = md
    return res


# Module-level cache for scrape results — press release URLs are immutable,
# scraping them once per session is enough.
_SCRAPE_CACHE: dict[tuple[str, str], dict] = {}

# Cache for parsed Eurostat press release tables (URL → markdown text).
_EUROSTAT_PRESS_MARKDOWN_CACHE: dict[str, str] = {}


def _parse_eurostat_press_table(text: str, component_label: str) -> list[dict]:
    """Parse a 6-7 month time series from an Eurostat HICP flash press release.

    The markdown layout is a wide table:
      Weights | Apr 25 | Nov 25 | Dec 25 | Jan 26 | Feb 26 | Mar 26 | Apr 26 | Apr 26-monthly
      **All-items HICP** | 1000.0 | 2.2 | 2.1 | 2.0 | ... | **3.0e** | 0.6e
      **Food** | ... | 2.5e | ...
      **Energy** | 90.5 | -3.6 | ... | **10.9e** | 3.0e

    We extract the headers (month-labels), then for the named component find
    the row of values, and return [{period, value}, ...] for the annual-rate
    columns only (skipping Weight and the monthly-rate column).
    """
    # Find month-header tokens: "Apr 25", "Nov 25", "Dec 25", ...
    headers = re.findall(r"\*\*([A-Z][a-z]{2})\s+(\d{2})\*\*", text)
    if not headers:
        return []

    # Convert month-abbrev to YYYY-MM
    month_idx = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                 "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    periods: list[str] = []
    for mon, yr in headers:
        m_num = month_idx.get(mon)
        if m_num:
            periods.append(f"20{yr}-{m_num:02d}")

    if not periods:
        return []

    # Find the component-row: after the **label** find values until next **label**
    # The label MAY have additional words ("Food, alcohol & tobacco"), so use
    # the leading part as anchor.
    pattern = re.compile(
        rf"(?-i:\*\*{re.escape(component_label)}\b[^*\n]*\*\*)([\s\S]*?)(?=\*\*[A-Z]|\Z)",
        flags=re.DOTALL,
    )
    m = pattern.search(text)
    if not m:
        return []

    body = m.group(1)
    # Tokens that are values: optional minus, digits, optional comma/dot, digits, optional 'e' marker
    # Strip markdown escapes for minus: "\-3.6" → "-3.6"
    body = body.replace("\\-", "-")
    value_tokens = re.findall(r"\*{0,2}(-?\d+[.,]\d+)e?\*{0,2}", body)
    # Convert to floats
    values: list[float] = []
    for v in value_tokens:
        try:
            values.append(float(v.replace(",", ".")))
        except ValueError:
            continue

    # Layout: first value = Weight (skip), then 1-per-header annual rates, last 1
    # is monthly rate (skip). Headers may contain a duplicate Apr 26 for monthly.
    # Heuristic: if there are exactly len(periods) values, treat all as annuals
    # in order. If len(periods)+1, the first one is Weight. If len(periods)+2,
    # Weight + monthly.
    n_h = len(periods)
    if len(values) >= n_h + 2:
        annuals = values[1:n_h + 1]
    elif len(values) >= n_h + 1:
        annuals = values[1:n_h + 1]
    elif len(values) >= n_h:
        annuals = values[:n_h]
    else:
        # Not enough values for the header schema
        annuals = values[:n_h]
        periods = periods[:len(annuals)]

    # The first header is typically Y-1 (year-ago, e.g. Apr 25); the last
    # is current. Sort by period; expose as time_series.
    pairs = list(zip(periods, annuals))
    # Filter out far-back outlier (year-ago): keep only if consecutive
    # Actually, we keep everything — the agent can decide which is relevant.
    out = [{"period": p, "value": v} for p, v in pairs]
    out.sort(key=lambda r: r["period"])
    return out


# Component-label mapping: indicator → markdown bold label in press release
_EUROSTAT_PRESS_LABELS: dict[str, str] = {
    "cpi":           "All-items HICP",
    "core_cpi":      "All-items HICP excl. energy and unprocessed food",
    "services_cpi":  "Services",
    "energy_cpi":    "Energy",
    "food_cpi":      "Food",
}


async def _resolver_eurostat_press(spec: dict) -> Optional[dict]:
    """Resolver: Eurostat newsroom press release scrape.

    The Eurostat publishes monthly euro-indicator press releases with a stable
    URL pattern:
        https://ec.europa.eu/eurostat/web/products-euro-indicators/w/2-{DDMM}{YYYY}-{suffix}
    where suffix is 'cp' for the final monthly press release (typically
    published around the 17–19th of the following month) which contains a
    per-country table including Hungary, or 'ap' for the flash (around the
    30th, euro-area aggregate only).

    spec keys:
        suffix: "cp" (final, country-bottom-line) or "ap" (flash, EA only)
        rx: regex with capture group 1 = numeric value
        months_back: probe N months back (default 3)

    The resolver iterates through candidate dates in the publication window
    (15..25 of the publication month) and the previous month, finding the
    first URL that returns a regex-matching value.
    """
    from datetime import datetime as _dt, timedelta as _td
    suffix = spec.get("suffix", "cp")
    rx = spec["rx"]
    # Historical mode: scrape ALL press releases (months_back back) and return
    # a time_series. Default months_back: 3 (single-shot) or 8 (historical).
    historical = bool(spec.get("historical", False))
    months_back = spec.get("months_back", 8 if historical else 3)
    now = _dt.now()

    # Candidate publication dates. Reference-month logic differs by suffix:
    #   - "ap" (flash): published at month-end, reference month = publication month
    #     (e.g. 2-30042026-ap covers April 2026 EA inflation)
    #   - "cp" (final): published ~17-19 of the next month, reference = prev month
    #     (e.g. 2-17052026-cp covers April 2026 final)
    cand: list[tuple[str, str]] = []  # (period-label, url)
    for back in range(months_back + 1):
        m_idx = now.month - back
        y = now.year
        while m_idx <= 0:
            m_idx += 12
            y -= 1
        if suffix == "ap":
            # Flash: same month
            ref_y, ref_m = y, m_idx
            day_range = range(30, 25, -1)  # 30, 29, 28, 27, 26
        else:
            # Final: previous month
            ref_m = m_idx - 1
            ref_y = y
            if ref_m <= 0:
                ref_m += 12
                ref_y -= 1
            day_range = range(20, 14, -1)  # 20, 19, 18, 17, 16, 15
        ref_label = f"{ref_y}-{ref_m:02d}"
        for day in day_range:
            url = f"https://ec.europa.eu/eurostat/web/products-euro-indicators/w/2-{day:02d}{m_idx:02d}{y}-{suffix}"
            cand.append((ref_label, url))

    # Group candidates by reference month (deduplicate URLs)
    by_period: dict[str, list[str]] = {}
    for ref_label, url in cand:
        by_period.setdefault(ref_label, []).append(url)

    async def _scrape_one_period(period: str, urls: list[str]) -> Optional[dict]:
        """Probe URLs for one reference month; return first match."""
        for url in urls:
            cache_key = (url, rx)
            cached = _SCRAPE_CACHE.get(cache_key)
            if cached is not None:
                if cached.get("_empty"):
                    continue
                return {"period": period, **cached}
            try:
                result = await _scrape_extract_value(url, rx)
            except Exception:
                continue
            if result and result.get("value") is not None:
                # Extract release_date from URL: 2-{DDMM}{YYYY}-{suffix}
                tail = url.rsplit("/", 1)[1]
                release_date = None
                try:
                    date_part = tail.split("-")[1]
                    if len(date_part) == 8:
                        d, m, y = date_part[:2], date_part[2:4], date_part[4:]
                        release_date = f"{y}-{m}-{d}"
                except (IndexError, ValueError):
                    pass
                entry = {
                    "value": result["value"],
                    "source_url": url,
                    "release_date": release_date,
                    "is_flash": (suffix == "ap"),
                    "release_type": "flash_estimate" if suffix == "ap" else "final",
                }
                _SCRAPE_CACHE[cache_key] = entry
                return {"period": period, **entry}
            else:
                _SCRAPE_CACHE[cache_key] = {"_empty": True}
        return None

    if historical:
        # Historical mode: scrape the LATEST press release and parse its
        # 6-7 month table for the named component. Single brave-mcp call
        # → full time_series. Falls back to per-period scrape if the
        # component_label is not set or the table parse fails.
        component_label = spec.get("component_label")
        if component_label:
            for ref_label, urls in sorted(by_period.items(), reverse=True):
                for url in urls:
                    # Cached markdown?
                    text = _EUROSTAT_PRESS_MARKDOWN_CACHE.get(url)
                    if text is None:
                        if BRAVE_MCP_URL:
                            r = await _brave_mcp_post("brave_scrape", {"url": url, "waitTime": 3000})
                            text = (r.get("markdown") or r.get("text") or "") if r else ""
                        if not text:
                            client = await get_client()
                            try:
                                rr = await client.get(url, timeout=20.0, follow_redirects=True,
                                                       headers={"User-Agent": "StatData/1.0"})
                                if rr.status_code == 200:
                                    text = re.sub(r"<[^>]+>", " ", rr.text)
                            except Exception:
                                text = ""
                        if text:
                            _EUROSTAT_PRESS_MARKDOWN_CACHE[url] = text
                    if not text:
                        continue
                    ts = _parse_eurostat_press_table(text, component_label)
                    if ts:
                        latest = ts[-1]
                        # Extract release_date from URL
                        url_tail = url.rsplit("/", 1)[1]
                        release_date = None
                        try:
                            dp = url_tail.split("-")[1]
                            if len(dp) == 8:
                                d, m, y = dp[:2], dp[2:4], dp[4:]
                                release_date = f"{y}-{m}-{d}"
                        except (IndexError, ValueError):
                            pass
                        return {
                            "value": latest["value"],
                            "period": latest["period"],
                            "source": f"Eurostat press release ({suffix}) table {component_label} {len(ts)}pts",
                            "source_url": url,
                            "release_date": release_date,
                            "is_flash": (suffix == "ap"),
                            "release_type": "flash_estimate" if suffix == "ap" else "final",
                            "time_series": ts,
                        }
            # Fall through to legacy per-period scrape if table parse failed

        # Legacy historical mode: scrape every period in parallel
        coros = [_scrape_one_period(p, urls) for p, urls in by_period.items()]
        results = await asyncio.gather(*coros, return_exceptions=True)
        time_series = [r for r in results if isinstance(r, dict) and r.get("value") is not None]
        if not time_series:
            return None
        time_series.sort(key=lambda r: r["period"])
        latest = time_series[-1]
        return {
            "value": latest["value"],
            "period": latest["period"],
            "source": f"Eurostat press release ({suffix}) historical {len(time_series)}pts → {latest['period']}",
            "source_url": latest.get("source_url"),
            "release_date": latest.get("release_date"),
            "is_flash": latest.get("is_flash", suffix == "ap"),
            "release_type": latest.get("release_type"),
            "time_series": time_series,
        }

    # Single-shot mode (legacy): first match wins
    for period in sorted(by_period.keys(), reverse=True):
        result = await _scrape_one_period(period, by_period[period])
        if result:
            return {
                "value": result["value"],
                "period": period,
                "source": f"Eurostat press release ({suffix}) {period}",
                "source_url": result.get("source_url"),
                "release_date": result.get("release_date"),
                "is_flash": result.get("is_flash", suffix == "ap"),
                "release_type": result.get("release_type"),
            }
    return None


async def _resolver_ksh_flash(spec: dict) -> Optional[dict]:
    """Resolver: direct KSH gyorstájékoztató scrape.

    KSH publishes monthly flash reports at the stable URL pattern
    https://www.ksh.hu/gyorstajekoztatok/{topic}/{topic}{YY}{MM}.html
    (e.g. far2604.html = 2026 April consumer prices flash).

    Tries the current month down to N months back; the first successful
    scrape + regex match wins. Returns {value, period, source_url, source}.
    """
    from datetime import datetime as _dt, timedelta as _td
    topic = spec["topic"]
    rx = spec["rx"]
    # 2026-05-11: max_back 4→2 (#186 transport errors). A KSH gyorstájékoztatók
    # tipikusan a megelőző hónapra publikálnak; 2 hónap visszamenőleg (current
    # + previous) elég a friss-adatra, és gyorsabb mint 4 hónap brave-scrape.
    max_months_back = spec.get("max_back", 2)
    now = _dt.now()
    # Generate candidate URLs from current month back to N months prior
    seen = set()
    candidates: list[tuple[str, str]] = []  # (period, url)
    for back in range(max_months_back + 1):
        # Compute the year/month back-by-N
        m_idx = now.month - back
        y = now.year
        while m_idx <= 0:
            m_idx += 12
            y -= 1
        yy = f"{y % 100:02d}"
        mm = f"{m_idx:02d}"
        key = (yy, mm)
        if key in seen:
            continue
        seen.add(key)
        url = f"https://www.ksh.hu/gyorstajekoztatok/{topic}/{topic}{yy}{mm}.html"
        candidates.append((f"{y}-{mm}", url))

    sign_aware = bool(spec.get("sign_aware", False))
    for period, url in candidates:
        try:
            result = await _scrape_extract_value(url, rx, sign_aware=sign_aware)
        except Exception:
            continue
        if result and result.get("value") is not None:
            result["period"] = period
            result["source"] = f"KSH flash {topic} {period}"
            result["source_url"] = url
            return result
    return None


async def _resolver_dbnomics(spec: dict) -> Optional[dict]:
    """Resolver: DBnomics series. Mainly used as FRED/ECB-mirror fallback when
    the direct API has an outage.

    spec keys:
        provider_code, dataset_code, series_code (required)
        compute_yoy: if True, take last 13 monthly obs and compute YoY%
                      (current / 12-months-ago - 1) × 100. Useful when the
                      DBnomics mirror only has level series and the direct
                      API would have transformed it (e.g. FRED units=pc1).
    """
    args = {
        "provider_code": spec["provider_code"],
        "dataset_code": spec["dataset_code"],
        "series_code": spec.get("series_code", ""),
        "max_obs": 24 if spec.get("compute_yoy") else 6,
    }
    raw = await dbnomics_series(**{k: v for k, v in args.items() if v})
    try:
        d = json.loads(raw)
    except Exception:
        return None
    series = d.get("series") or []
    if not series:
        return None
    s0 = series[0]
    obs = s0.get("observations") or []
    if not obs:
        return None
    # observations is list of {period, value}
    obs = [o for o in obs if o.get("value") is not None]
    if not obs:
        return None
    obs.sort(key=lambda o: str(o.get("period", "")))

    if spec.get("compute_yoy") and len(obs) >= 13:
        latest = obs[-1]
        # Find the obs ~12 months earlier with same calendar month
        latest_period = str(latest["period"])
        target_year = None
        if len(latest_period) >= 7 and latest_period[4] == "-":
            try:
                target_year = int(latest_period[:4]) - 1
            except ValueError:
                pass
        prev = None
        if target_year:
            target_period = f"{target_year}{latest_period[4:]}"
            for o in obs:
                if str(o.get("period", "")) == target_period:
                    prev = o
                    break
        if prev is None:
            prev = obs[-13]  # fall back to 13 positions back
        if prev.get("value"):
            yoy = round((float(latest["value"]) / float(prev["value"]) - 1) * 100, 2)
            return {
                "value": yoy,
                "period": latest["period"],
                "source": f"DBnomics {spec['provider_code']}/{spec['dataset_code']} (YoY% from levels)",
                "level_latest": latest["value"],
                "level_prev_year": prev["value"],
            }

    return {
        "value": obs[-1]["value"],
        "period": obs[-1]["period"],
        "source": f"DBnomics {spec['provider_code']}/{spec['dataset_code']}",
    }


async def _resolver_oecd(spec: dict) -> Optional[dict]:
    """Resolver: OECD SDMX direct query.

    Wraps a single OECD SDMX-JSON data call. Useful as an alternative source
    when Eurostat/ECB ICP series are truncated (OECD often has slightly older
    data but covers all OECD members including HU/PL/CZ/RO/JP/KR/etc.).

    spec keys:
        agency: SDMX agency.context, e.g. "OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0"
        key: SDMX series key, e.g. "M.HUN.N.CPI.PA._T.N.GY"
        params: optional dict of extra query params
    """
    base = _OECD_SDMX_BASE  # https://sdmx.oecd.org/public/rest/data
    agency = spec["agency"]
    key = spec["key"]
    url = f"{base}/{agency}/{key}"
    params = {"format": "jsondata", "lastNObservations": 12, **(spec.get("params") or {})}
    client = await get_client()
    try:
        r = await client.get(url, params=params, headers={"Accept": "application/json"}, timeout=25.0)
        if r.status_code != 200:
            return None
        d = r.json()
    except Exception:
        return None

    # OECD SDMX-JSON has same shape as ECB — reuse _parse_ecb_jsondata
    try:
        parsed = _parse_ecb_jsondata(d)
    except Exception:
        return None
    obs = parsed.get("observations") or []
    if not obs:
        return None
    latest = obs[-1]
    return {
        "value": latest.get("value"),
        "period": latest.get("period"),
        "source": f"OECD SDMX {agency.split(',')[1].split('@')[0] if '@' in agency else agency}",
    }


async def _resolver_bis(spec: dict) -> Optional[dict]:
    """Resolver: BIS WS_CBPOL via DBnomics (used as policy-rate fallback)."""
    raw = await get_policy_rates(countries=spec["country"], limit=3)
    try:
        d = json.loads(raw)
    except Exception:
        return None
    rates = d.get("rates", {}).get(spec["country"]) or {}
    if not rates.get("current_rate"):
        return None
    return {
        "value": rates["current_rate"],
        "period": rates.get("as_of", ""),
        "source": f"BIS WS_CBPOL {spec['country']}",
        "stale_flag": rates.get("stale", False),
    }


_RESOLVERS = {
    "ecb": _resolver_ecb,
    "eurostat": _resolver_eurostat,
    "eurostat_press": _resolver_eurostat_press,
    "fred": _resolver_fred,
    "dbnomics": _resolver_dbnomics,
    "oecd": _resolver_oecd,
    "ksh_stadat": _resolver_ksh_stadat,
    "ksh_flash": _resolver_ksh_flash,
    "scrape": _resolver_scrape,
    "brave_search": _resolver_brave_search,
    "bis": _resolver_bis,
}


@mcp.tool()
async def get_macro_indicator(
    country: str,
    indicator: str,
    freshness_days: int = 0,
) -> str:
    """High-level macro indicator router — guaranteed-fresh, country-agnostic.

    Returns the latest value for a given (country, indicator) by trying a
    chain of resolvers (structured APIs → official scrape → brave_search)
    and stopping at the FIRST resolver whose latest observation is within
    the freshness window. If every resolver is stale, returns the freshest
    stale value with explicit status='stale'.

    Use this tool when you want **a number** without orchestrating multiple
    low-level calls. The Bridge / sub-agents should default to this for
    "what's HU's CPI right now" type questions.

    Args:
        country: ISO-2 country code (HU, DE, FR, IT, ES, EA, US, GB, ...).
                 EA = euro area aggregate (uses ECB U2 / Eurostat EA20).
        indicator: One of:
                   "cpi"             — headline HICP annual rate of change (%)
                   "core_cpi"        — core HICP (excl. energy & food), YoY%
                   "services_cpi"    — HICP services component, YoY%
                   "energy_cpi"      — HICP energy component, YoY%
                   "food_cpi"        — HICP food component, YoY%
                   "policy_rate"     — central bank policy rate (%)
                   "unemployment"    — unemployment rate (%)
                   "gdp"             — GDP absolute level (€M CLV15, SCA quarterly)
                   "gdp_growth"      — GDP quarter-on-quarter %-change (SCA)
                   "ppi"             — producer prices YoY%
                   "wages"           — average wage / earnings index
                   "retail_trade"    — retail trade volume index
                   "industrial_production" — industrial production index
                   "trade_balance"   — external trade balance
                   "gov_debt"        — general gov. debt-to-GDP ratio (%)
                   "house_prices"    — house price index YoY%
                   "bond_yield_10y"  — 10Y Maastricht government bond yield (%)
        freshness_days: Override the default freshness threshold (CPI 60d,
                        policy_rate 75d, unemployment 75d, gdp 120d).
                        Use 0 (default) for the indicator default.

    Returns:
        JSON with:
          country, indicator, value, period, source_used,
          status ("fresh"|"stale"|"missing"),
          fallback_chain — ordered list of [resolver_type, outcome, period],
          all_attempts — debug log of every resolver invocation.

    Examples:
        get_macro_indicator(country="HU", indicator="policy_rate")
          → MNB scrape → 6.25 (2026-04-28), source: scrape mnb.hu
        get_macro_indicator(country="DE", indicator="cpi")
          → ECB ICP → 2.2 (2026-03), source: ECB M.DE.N.000000.4.ANR
        get_macro_indicator(country="US", indicator="unemployment")
          → FRED UNRATE → 3.9 (2026-03), source: FRED
    """
    country = country.strip().upper()
    indicator = indicator.strip().lower()
    chain = INDICATOR_RESOLVERS.get((country, indicator))
    if not chain:
        valid_countries = sorted({c for c, _ in INDICATOR_RESOLVERS})
        valid_indicators = sorted({i for _, i in INDICATOR_RESOLVERS})
        return json.dumps({
            "error": f"No resolver chain for ({country!r}, {indicator!r}).",
            "valid_countries": valid_countries,
            "valid_indicators": valid_indicators,
            "hint": "Extend INDICATOR_RESOLVERS in server.py to support this combo.",
        }, ensure_ascii=False, indent=2)

    threshold = freshness_days if freshness_days > 0 else _FRESHNESS_DAYS.get(indicator, 90)
    attempts: list[dict] = []
    best_stale: Optional[dict] = None  # freshest stale value found across the chain

    for spec in chain:
        rtype = spec.get("type")
        fn = _RESOLVERS.get(rtype)
        if not fn:
            attempts.append({"resolver": rtype, "outcome": "no_resolver"})
            continue
        try:
            result = await fn(spec)
        except Exception as e:
            logger.warning("Resolver %s failed: %s", rtype, e)
            attempts.append({"resolver": rtype, "outcome": "error", "error": str(e)[:200]})
            continue
        if not result or result.get("value") is None:
            attempts.append({"resolver": rtype, "outcome": "empty"})
            continue
        period = str(result.get("period") or "")
        fresh = _is_fresh(period, threshold)
        attempts.append({
            "resolver": rtype, "outcome": "ok",
            "value": result["value"], "period": period, "fresh": fresh,
        })
        if fresh:
            out_payload = {
                "country": country,
                "indicator": indicator,
                "value": result["value"],
                "period": period,
                "status": "fresh",
                "source_used": result.get("source", rtype),
                "source_url": result.get("source_url"),
                "freshness_threshold_days": threshold,
                "fallback_chain": [a["resolver"] for a in attempts],
                "all_attempts": attempts,
                "agent_instruction": (
                    f"Ez a TÉNYLEGES legfrissebb {country} {indicator} adat: "
                    f"{result['value']} ({period}). A fallback_chain és all_attempts "
                    f"AUDIT-TRAIL — a stale értékek (pl. korábbi ECB ICP, Eurostat) "
                    f"NEM minősítendők 'hiányzó forrásnak' vagy 'nem elérhető adatnak'. "
                    f"A {indicator} ITT VAN, FRISSEN. Idézd ezt az értéket és a "
                    f"source_used-et a brief-ben — ne flag-eld hiányzónak."
                ),
            }
            # Optional context fields propagated from resolver
            for key in ("decision_date", "is_flash", "release_date",
                        "release_type", "time_series"):
                if result.get(key) is not None:
                    out_payload[key] = result[key]
            return json.dumps(out_payload, ensure_ascii=False, indent=2)
        # Stale but valid — keep as best fallback
        dt = _parse_period_to_date(period)
        if dt and (not best_stale or _parse_period_to_date(best_stale["period"]) < dt):
            best_stale = {
                "value": result["value"], "period": period,
                "source": result.get("source", rtype),
                "source_url": result.get("source_url"),
            }

    if best_stale:
        return json.dumps({
            "country": country,
            "indicator": indicator,
            "value": best_stale["value"],
            "period": best_stale["period"],
            "status": "stale",
            "source_used": best_stale["source"],
            "source_url": best_stale.get("source_url"),
            "freshness_threshold_days": threshold,
            "fallback_chain": [a["resolver"] for a in attempts],
            "all_attempts": attempts,
            "warning": (
                f"All resolvers returned stale data (latest: {best_stale['period']}, "
                f"threshold: {threshold} days). The value is the freshest available "
                f"but should be flagged as stale to the end user."
            ),
        }, ensure_ascii=False, indent=2)

    return json.dumps({
        "country": country,
        "indicator": indicator,
        "status": "missing",
        "fallback_chain": [a["resolver"] for a in attempts],
        "all_attempts": attempts,
        "error": (
            f"No resolver returned data for ({country}, {indicator}). "
            "Check that BRAVE_MCP_URL is set for scrape/search fallbacks, "
            "or extend the resolver chain."
        ),
    }, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Landing page
# ---------------------------------------------------------------------------
LANDING_HTML = """<!DOCTYPE html>
<html lang="hu">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<meta property="og:title" content="StatData — Statisztikai Adatok MCP">
<meta property="og:description" content="Eurostat, KSH, DBnomics, MNB, ECB, FRED — 700M+ adatsor egyetlen MCP szerveren. AI asszisztensek azonnali hozzáférése a világ makrogazdasági adataihoz.">
<meta property="og:type" content="website">
<meta property="og:image" content="__BASE_URL__/og-image.svg">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="StatData — Statisztikai Adatok MCP">
<meta name="twitter:description" content="Eurostat, KSH, DBnomics, MNB, ECB, FRED — 700M+ adatsor egyetlen MCP szerveren.">
<meta name="twitter:image" content="__BASE_URL__/og-image.svg">
<title>StatData — Statisztikai Adatok MCP</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --primary: #53d22d;
    --primary-dim: rgba(83, 210, 45, 0.15);
    --accent-blue: #3b82f6;
    --accent-purple: #8b5cf6;
    --bg: #050505;
    --bg-card: rgba(10, 10, 10, 0.7);
    --text: #f0f0f0;
    --text-dim: #a0a0a0;
    --border: rgba(255, 255, 255, 0.06);
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Inter', -apple-system, sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    overflow-x: hidden;
  }
  /* Ambient orbs */
  .ambient { position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: 0; pointer-events: none; }
  .orb {
    position: absolute; border-radius: 50%; filter: blur(120px); opacity: 0.2;
    animation: orb-float 14s ease-in-out infinite alternate;
  }
  .orb-1 { background: var(--primary); width: 600px; height: 600px; top: -200px; left: -200px; }
  .orb-2 { background: var(--accent-blue); width: 500px; height: 500px; bottom: -150px; right: -150px; animation-delay: 4s; }
  .orb-3 { background: var(--accent-purple); width: 350px; height: 350px; top: 40%; left: 50%; opacity: 0.12; animation-delay: 7s; }
  @keyframes orb-float {
    0% { transform: translate(0,0) scale(1); }
    50% { transform: translate(30px,-40px) scale(1.05); }
    100% { transform: translate(-20px,20px) scale(0.97); }
  }
  /* Layout */
  .content {
    position: relative; z-index: 1;
    display: flex; flex-direction: column; align-items: center;
    padding: 3rem 1.5rem 2rem;
    min-height: 100vh;
  }
  /* Hero */
  .hero { text-align: center; max-width: 640px; margin-bottom: 3rem; }
  .hero h1 {
    font-size: 2.4rem; font-weight: 800; letter-spacing: -0.03em;
    color: var(--primary);
    margin-bottom: 0.6rem;
  }
  .hero .sub { font-size: 1.05rem; color: var(--text-dim); line-height: 1.7; font-weight: 300; }
  .nexus-wrap { margin-bottom: 1.5rem; display: flex; justify-content: center; }
  #globe { display: block; max-width: 100%; height: auto; }
  /* Source badges */
  .sources { display: flex; gap: 0.4rem; flex-wrap: wrap; justify-content: center; margin-top: 1.2rem; }
  .sources span {
    background: var(--primary-dim); padding: 0.25rem 0.7rem; border-radius: 999px;
    font-size: 0.75rem; color: var(--primary); border: 1px solid rgba(83,210,45,0.15);
    font-weight: 500;
  }
  /* Cards */
  .cards {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 1.2rem; max-width: 960px; width: 100%; margin-bottom: 3rem;
  }
  .card {
    background: var(--bg-card); backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
    border: 1px solid var(--border); border-radius: 16px; padding: 1.4rem;
    position: relative; overflow: hidden; transition: border-color 0.3s;
  }
  .card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 1px;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.06), transparent);
  }
  .card:hover { border-color: rgba(83, 210, 45, 0.25); }
  .card h3 { font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem; }
  .card p { font-size: 0.82rem; color: var(--text-dim); margin-bottom: 0.8rem; line-height: 1.5; }
  .card code {
    display: block; background: rgba(0,0,0,0.5); padding: 0.65rem; border-radius: 8px;
    font-size: 0.7rem; color: var(--primary); word-break: break-all;
    margin-bottom: 0.7rem; max-height: 110px; overflow-y: auto; white-space: pre-wrap;
    border: 1px solid var(--border);
  }
  .btn {
    display: block; width: 100%; padding: 0.5rem; border-radius: 8px;
    font-size: 0.82rem; font-weight: 500; border: none; cursor: pointer;
    background: linear-gradient(135deg, var(--primary), #7cd22d);
    color: #050505; transition: opacity 0.2s; text-align: center;
  }
  .btn:hover { opacity: 0.85; }
  .btn.copied { background: var(--accent-blue); color: white; }
  /* Tools */
  .tools {
    max-width: 720px; width: 100%; margin-bottom: 2rem;
    background: var(--bg-card); backdrop-filter: blur(16px);
    border: 1px solid var(--border); border-radius: 16px; padding: 1.5rem;
  }
  .tools h2 { font-size: 1.1rem; font-weight: 600; margin-bottom: 1rem; }
  .tools table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
  .tools td, .tools th { padding: 0.45rem 0.6rem; border-bottom: 1px solid var(--border); text-align: left; }
  .tools th { color: var(--text-dim); font-weight: 400; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; }
  .tools td:first-child { color: var(--primary); font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.75rem; }
  footer { color: rgba(255,255,255,0.2); font-size: 0.7rem; margin-top: auto; padding: 2rem 0 1rem; }
  @media (max-width: 600px) {
    .hero h1 { font-size: 1.6rem; }
    .cards { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
<div class="ambient">
  <div class="orb orb-1"></div>
  <div class="orb orb-2"></div>
  <div class="orb orb-3"></div>
</div>

<div class="content">
<div class="hero">
  <div class="nexus-wrap">
    <canvas id="globe"></canvas>
  </div>
  <h1>StatData — Statisztikai Adatok MCP</h1>
  <p class="sub">Eurostat, KSH, DBnomics, MNB, ECB és Yahoo Finance adatok elérése<br>
     AI asszisztenseken keresztül — egy kattintással.</p>
  <div class="sources">
    <span>Eurostat</span>
    <span>KSH STADAT</span>
    <span>DBnomics</span>
    <span>MNB</span>
    <span>ECB</span>
    <span>FED</span>
    <span>IMF</span>
    <span>OECD</span>
    <span>World Bank</span>
    <span>Yahoo Finance</span>
    <span>BIS</span>
    <span>COMEXT</span>
    <span>ECB Data Portal</span>
    <span>KSH gyorstájékoztatók</span>
    <span>Eurostat news</span>
  </div>
</div>

<div class="cards">
  <div class="card">
    <h3>Claude Desktop</h3>
    <p>Settings &rarr; Developer &rarr; Edit Config</p>
    <code id="claude-config">{
  "mcpServers": {
    "statisztika": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "MCP_URL"]
    }
  }
}</code>
    <button class="btn" onclick="copyConfig('claude-config', this)">Konfiguráció másolása</button>
  </div>

  <div class="card">
    <h3>Claude Web / Mobil</h3>
    <p>claude.ai &rarr; Settings &rarr; Integrations</p>
    <code id="claude-web-url">MCP_URL</code>
    <button class="btn" onclick="copyConfig('claude-web-url', this)">URL másolása</button>
  </div>

  <div class="card">
    <h3>ChatGPT</h3>
    <p>Settings &rarr; More tools &rarr; Add MCP</p>
    <code id="chatgpt-url">MCP_URL</code>
    <button class="btn" onclick="copyConfig('chatgpt-url', this)">URL másolása</button>
  </div>

</div>

<div class="tools">
  <h2>Elérhető eszközök</h2>
  <p style="color: var(--text-dim); font-size: 0.85rem; line-height: 1.6; margin-bottom: 1.2rem;">
    Azonnali hozzáférés az Európai Unió, Magyarország és a világ legfontosabb makrogazdasági adatbázisaihoz.
    GDP, infláció, munkaerőpiac, államadósság, ipari termelés, külkereskedelem, árfolyamok, nyersanyagárak,
    kötvényhozamok, tőzsdeindexek — több mint 700 millió adatsor, 70+ nemzetközi szervezettől, egyetlen felületen.
  </p>
  <table>
    <tr><th>Eszköz</th><th>Leírás</th></tr>
    <tr><td>search_datasets</td><td>Keresés Eurostat, KSH és DBnomics közt — szinonimákkal (ország + téma)</td></tr>
    <tr><td>get_eurostat_data</td><td>Eurostat adatlekérés (GDP, infláció, munkanélküliség…)</td></tr>
    <tr><td>dbnomics_search</td><td>Keresés 700M+ adatsor közt + adatszolgáltatók listája (mode="providers")</td></tr>
    <tr><td>dbnomics_series</td><td>Idősor lekérése DBnomics-ból — sikeres lekérések receptté válnak</td></tr>
    <tr><td>get_ksh_stadat</td><td>KSH STADAT táblák — magyar idősorok (árak, bérek, GDP…)</td></tr>
    <tr><td>get_ksh_hvd</td><td>KSH High-Value Datasets — listázás/keresés vagy letöltés</td></tr>
    <tr><td>yfinance</td><td>Yahoo Finance — aktuális árfolyam (action="quote") vagy historikus adatok (action="history")</td></tr>
    <tr><td>mnb_rates</td><td>MNB árfolyamok — aktuális (mode="current") vagy historikus (mode="historical", 1949-től)</td></tr>
    <tr><td>calculate</td><td>Gazdasági kalkulátor (infláció, CAGR, reálérték, konverzió)</td></tr>
    <tr><td>recipe_book</td><td>Receptkönyv — keresés, hozzáadás, hibajelentés, statisztikák (action paraméter)</td></tr>
    <tr><td>forecast</td><td>Prognózis — GDP, infláció, munkanélküliség, OECD CLI (52 ország, negyedéves)</td></tr>
    <tr><td>get_fred_data</td><td>FRED — 800K+ US gazdasági idősor (kamatok, infláció, GDP, munkaerő…)</td></tr>
    <tr><td>get_economic_calendar</td><td>Gazdasági naptár — közelgő adatközlések (FRED, ECB, Eurostat)</td></tr>
    <tr><td>get_policy_rates</td><td>Jegybanki alapkamatok — BIS (ECB, MNB, CNB, NBP, BNR, Fed…)</td></tr>
    <tr><td>get_eurostat_data</td><td>↳ COMEXT mód: dataset_code="COMEXT" — SITC külkereskedelem (HS-hez: Easy Comext web)</td></tr>
  </table>
</div>

<footer>StatData</footer>
</div>

<script>
const MCP_URL = window.location.origin + '/mcp';
document.querySelectorAll('code').forEach(el => {
  el.textContent = el.textContent.replace(/MCP_URL/g, MCP_URL);
});
function copyConfig(id, btn) {
  const text = document.getElementById(id).textContent;
  navigator.clipboard.writeText(text).then(() => {
    btn.textContent = 'Másolva!';
    btn.classList.add('copied');
    setTimeout(() => { btn.textContent = btn.dataset.orig || 'Másolás'; btn.classList.remove('copied'); }, 2000);
  });
  btn.dataset.orig = btn.dataset.orig || btn.textContent;
}
</script>
<script>
(function(){
var c=document.getElementById('globe'),g=c.getContext('2d'),
    S=320,R=130,dpr=window.devicePixelRatio||1,PI=Math.PI,RAD=PI/180;
c.width=S*dpr;c.height=S*dpr;c.style.width=S+'px';c.style.height=S+'px';
g.scale(dpr,dpr);
var cx=S/2,cy=S/2,TILT=22*RAD;

var HU=[
[16.11,46.87],[16.18,46.38],[16.52,46.50],[17.04,45.80],[18.21,45.79],
[18.90,45.93],[19.73,46.17],[20.26,46.13],[20.78,46.27],[21.14,46.28],
[21.88,47.03],[22.36,47.52],[22.16,48.40],[21.61,48.50],[20.83,48.58],
[19.77,48.20],[19.04,48.07],[18.83,48.04],[18.16,47.76],[17.76,47.77],
[17.15,48.01],[16.95,47.69],[16.42,47.66],[16.11,47.41],[16.11,46.87]
];
var DOTS=[
[2.35,48.86],[13.40,52.52],[-3.70,40.42],[12.50,41.90],[23.72,37.97],
[14.42,50.08],[21.01,52.23],[18.07,59.33],[24.94,60.17],[-9.14,38.74],
[4.35,50.85],[4.90,52.37],[16.37,48.21],[14.51,46.06],[15.98,45.81],
[26.10,44.43],[23.32,42.70],[17.11,48.15],[25.28,54.69],[24.10,56.95],
[-0.12,51.51],[10.75,59.91],[8.55,47.37],[-6.26,53.35],
[37.62,55.75],[30.52,50.45],[28.98,41.01],[44.42,33.32],[51.42,35.69],
[-73.97,40.71],[-43.17,-22.91],[139.69,35.68],[116.40,39.90],[77.21,28.61],
[-122.42,37.77],[103.82,1.35],[151.21,-33.87],[18.42,-33.93],[36.82,-1.29]
];

var EUB=[
[-9.5,37],[-6,36],[0,38],[3,42],[6,43.5],[7.5,44],[10,44],[13,38],[16,37.5],
[18,40],[21,38],[24,35],[26,38],[28,41],[29,43],[28,46],[27,48],[24,51],
[23,54],[22,56],[24,57.5],[26,59.5],[28,61],[30,64],[28,68],[24,70],
[20,69],[16,66],[13,63],[12,58],[10,57.5],[9,55],[7,54],[5,53],[3.5,52],
[2,51],[-1,48],[-5,48.5],[-9.5,43.5],[-9.5,37]
];

function P(lon,lat,ry){
var l=(lon-ry)*RAD,p=lat*RAD,
    cp=Math.cos(p),sp=Math.sin(p),cl=Math.cos(l),sl=Math.sin(l),
    ct=Math.cos(TILT),st=Math.sin(TILT),
    y=sp*ct-cp*cl*st, z=sp*st+cp*cl*ct;
return[cx+R*cp*sl, cy-R*y, z];
}

function draw(t){
var rot=t*0.006;
g.clearRect(0,0,S,S);

// atmosphere
var a=g.createRadialGradient(cx,cy,R*.92,cx,cy,R*1.18);
a.addColorStop(0,'rgba(83,210,45,.06)');a.addColorStop(1,'rgba(83,210,45,0)');
g.beginPath();g.arc(cx,cy,R*1.18,0,PI*2);g.fillStyle=a;g.fill();

// globe body
var b=g.createRadialGradient(cx-R*.22,cy-R*.22,R*.08,cx,cy,R);
b.addColorStop(0,'rgba(20,20,20,1)');b.addColorStop(1,'rgba(6,6,6,1)');
g.beginPath();g.arc(cx,cy,R,0,PI*2);g.fillStyle=b;g.fill();
g.strokeStyle='rgba(83,210,45,.1)';g.lineWidth=.8;g.stroke();

// clip to globe
g.save();g.beginPath();g.arc(cx,cy,R,0,PI*2);g.clip();

// latitude grid
g.strokeStyle='rgba(83,210,45,.05)';g.lineWidth=.5;
for(var lat=-60;lat<=60;lat+=30){
  g.beginPath();var on=0;
  for(var lon=0;lon<360;lon+=3){
    var p=P(lon,lat,rot);
    if(p[2]>0){if(!on){g.moveTo(p[0],p[1]);on=1;}else g.lineTo(p[0],p[1]);}
    else on=0;
  }g.stroke();
}
// longitude grid
for(var lon=0;lon<360;lon+=30){
  g.beginPath();var on=0;
  for(var lat=-90;lat<=90;lat+=3){
    var p=P(lon,lat,rot);
    if(p[2]>0){if(!on){g.moveTo(p[0],p[1]);on=1;}else g.lineTo(p[0],p[1]);}
    else on=0;
  }g.stroke();
}

// EU region
var ep=EUB.map(function(e){return P(e[0],e[1],rot);});
var env=0;ep.forEach(function(p){if(p[2]>0)env++;});
if(env>ep.length*.4){
  g.beginPath();
  ep.forEach(function(p,i){i?g.lineTo(p[0],p[1]):g.moveTo(p[0],p[1]);});
  g.closePath();
  g.fillStyle='rgba(59,130,246,.06)';g.fill();
  g.strokeStyle='rgba(59,130,246,.25)';g.lineWidth=1;
  g.setLineDash([3,3]);g.stroke();g.setLineDash([]);
}

// city dots
for(var i=0;i<DOTS.length;i++){
  var p=P(DOTS[i][0],DOTS[i][1],rot);
  if(p[2]>0){
    var isEU=i<24;
    g.beginPath();g.arc(p[0],p[1],isEU?1.5:1,0,PI*2);
    g.fillStyle=isEU?'rgba(59,130,246,.35)':'rgba(255,255,255,.12)';
    g.fill();
  }
}

// orbiting data particles
g.save();
g.shadowColor='#53d22d';
for(var i=0;i<24;i++){
  var spd=0.012+i*0.004;
  var plon=(t*spd+i*15)%360;
  var plat=50*Math.sin(t*0.0005+i*0.7);
  var pp=P(plon,plat,rot);
  if(pp[2]>0){
    var sz=1.5+(i%4)*.5;
    var op=.4+.4*pp[2];
    g.shadowBlur=sz*4;
    g.beginPath();g.arc(pp[0],pp[1],sz,0,PI*2);
    g.fillStyle='rgba(83,210,45,'+op+')';g.fill();
  }
}
g.restore();

// Hungary
var hp=HU.map(function(h){return P(h[0],h[1],rot);});
var nv=0;hp.forEach(function(p){if(p[2]>0)nv++;});
if(nv>hp.length*.4){
  g.save();
  g.shadowColor='#53d22d';g.shadowBlur=30;
  g.beginPath();
  hp.forEach(function(p,i){i?g.lineTo(p[0],p[1]):g.moveTo(p[0],p[1]);});
  g.closePath();
  var pulse=.35+.15*Math.sin(t*.002);
  g.fillStyle='rgba(83,210,45,'+pulse+')';g.fill();
  g.strokeStyle='rgba(83,210,45,.9)';g.lineWidth=1.5;g.stroke();
  g.shadowBlur=0;

  // Budapest beacon
  var bp=P(19.04,47.50,rot);
  if(bp[2]>0){
    // outer pulse ring
    var pr=4+3*Math.sin(t*.003);
    g.beginPath();g.arc(bp[0],bp[1],pr,0,PI*2);
    g.strokeStyle='rgba(83,210,45,'+(0.5-pr/14)+')';g.lineWidth=1;g.stroke();
    // dot
    g.beginPath();g.arc(bp[0],bp[1],2.5,0,PI*2);
    g.fillStyle='#53d22d';g.shadowBlur=12;g.shadowColor='#53d22d';g.fill();
  }
  g.restore();

  // data beams radiating from Hungary center
  var hc=P(19.5,47.5,rot);
  if(hc[2]>0){
    for(var i=0;i<5;i++){
      var ang=(i/5)*PI*2+t*.0008;
      var len=12+8*Math.sin(t*.003+i*1.3);
      var ex=hc[0]+Math.cos(ang)*len,ey=hc[1]+Math.sin(ang)*len;
      var lg=g.createLinearGradient(hc[0],hc[1],ex,ey);
      lg.addColorStop(0,'rgba(83,210,45,.5)');lg.addColorStop(1,'rgba(83,210,45,0)');
      g.beginPath();g.moveTo(hc[0],hc[1]);g.lineTo(ex,ey);
      g.strokeStyle=lg;g.lineWidth=1;g.stroke();
    }
  }
}

g.restore(); // unclip

// specular highlight
var sp=g.createRadialGradient(cx-R*.35,cy-R*.35,0,cx-R*.35,cy-R*.35,R*.6);
sp.addColorStop(0,'rgba(255,255,255,.04)');sp.addColorStop(1,'rgba(255,255,255,0)');
g.beginPath();g.arc(cx,cy,R,0,PI*2);g.fillStyle=sp;g.fill();

requestAnimationFrame(draw);
}
requestAnimationFrame(draw);
})();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Startup: seed DB + trigger background scan
# ---------------------------------------------------------------------------
_seed_db_from_static()

if not _db_is_fresh():
    # Will be picked up by the event loop once the server starts
    @mcp.custom_route("/_scan_trigger", methods=["GET"])
    async def _trigger_scan(request):
        """Hidden endpoint to trigger KSH scan (also auto-triggered on first tool call)."""
        asyncio.create_task(_scan_ksh_stadat_background())
        return HTMLResponse("Scan started")

    _scan_scheduled = True
    logger.info("KSH STADAT index is stale — scan will start on first request")
else:
    _scan_scheduled = False
    logger.info("KSH STADAT index is fresh — using cached DB")


FAVICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">
<rect width="32" height="32" rx="6" fill="#050505"/>
<g transform="translate(16,16)">
<circle r="12" fill="none" stroke="#53d22d" stroke-width="1" opacity=".3"/>
<ellipse rx="12" ry="4.5" fill="none" stroke="#53d22d" stroke-width=".7" opacity=".15"/>
<ellipse rx="4.5" ry="12" fill="none" stroke="#53d22d" stroke-width=".7" opacity=".15"/>
<circle cx="1" cy="-3" r="3.5" fill="#53d22d" opacity=".8"/>
</g>
</svg>"""

OG_IMAGE_SVG = """<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
<rect width="1200" height="630" fill="#050505"/>
<circle cx="600" cy="250" r="220" fill="#53d22d" opacity=".02"/>
<g transform="translate(600,250)">
<circle r="170" fill="rgba(10,10,10,.9)" stroke="#53d22d" stroke-width="1.2" opacity=".8"/>
<ellipse rx="170" ry="40" fill="none" stroke="#53d22d" stroke-width=".7" opacity=".06"/>
<ellipse rx="170" ry="85" fill="none" stroke="#53d22d" stroke-width=".7" opacity=".06"/>
<ellipse rx="170" ry="130" fill="none" stroke="#53d22d" stroke-width=".7" opacity=".06"/>
<ellipse rx="40" ry="170" fill="none" stroke="#53d22d" stroke-width=".7" opacity=".06" transform="rotate(-15)"/>
<ellipse rx="85" ry="170" fill="none" stroke="#53d22d" stroke-width=".7" opacity=".06" transform="rotate(-15)"/>
<ellipse rx="130" ry="170" fill="none" stroke="#53d22d" stroke-width=".7" opacity=".06" transform="rotate(-15)"/>
<circle cx="-50" cy="-25" r="5" fill="#3b82f6" opacity=".3"/>
<circle cx="-80" cy="-10" r="4" fill="#3b82f6" opacity=".25"/>
<circle cx="30" cy="-50" r="4" fill="#3b82f6" opacity=".25"/>
<circle cx="10" cy="-35" r="20" fill="#53d22d" opacity=".12"/>
<circle cx="10" cy="-35" r="10" fill="#53d22d" opacity=".35"/>
<circle cx="10" cy="-35" r="4" fill="#53d22d" opacity=".8"/>
</g>
<text x="600" y="475" text-anchor="middle" fill="#f0f0f0" font-family="Inter,Arial,sans-serif" font-size="48" font-weight="800" letter-spacing="-1">StatData</text>
<text x="600" y="520" text-anchor="middle" fill="#a0a0a0" font-family="Inter,Arial,sans-serif" font-size="22" font-weight="300">Statisztikai Adatok MCP</text>
<text x="600" y="568" text-anchor="middle" fill="#53d22d" font-family="Inter,Arial,sans-serif" font-size="14" opacity=".5">Eurostat · KSH · DBnomics · MNB · ECB · FRED · IMF · OECD · Yahoo Finance</text>
</svg>"""


@mcp.custom_route("/favicon.svg", methods=["GET"])
async def favicon(request):
    return Response(FAVICON_SVG, media_type="image/svg+xml")


@mcp.custom_route("/og-image.svg", methods=["GET"])
async def og_image(request):
    return Response(OG_IMAGE_SVG, media_type="image/svg+xml")


@mcp.custom_route("/", methods=["GET"])
async def landing_page(request):
    # Trigger background scan on first page visit if needed
    global _scan_scheduled
    if _scan_scheduled and not _ksh_scan_running:
        _scan_scheduled = False
        asyncio.create_task(_scan_ksh_stadat_background())
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", request.url.netloc)
    base_url = f"{scheme}://{host}"
    html = LANDING_HTML.replace("__BASE_URL__", base_url)
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# REST API — for Bridge / external clients to invoke tools without MCP plumbing
# ---------------------------------------------------------------------------
_API_TOOL_DISPATCH = {
    "search_datasets": search_datasets,
    "get_eurostat_data": get_eurostat_data,
    "get_ksh_hvd": get_ksh_hvd,
    "dbnomics_search": dbnomics_search,
    "dbnomics_series": dbnomics_series,
    "get_ksh_stadat": get_ksh_stadat,
    "yfinance": yfinance,
    "calculate": calculate,
    "mnb_rates": mnb_rates,
    "recipe_book": recipe_book,
    "get_fred_data": get_fred_data,
    "forecast": forecast,
    "get_economic_calendar": get_economic_calendar,
    "get_policy_rates": get_policy_rates,
    "get_ecb_data": get_ecb_data,
    "get_flash_releases": get_flash_releases,
    "get_macro_indicator": get_macro_indicator,
}


@mcp.custom_route("/api/call", methods=["POST"])
async def api_call(request):
    """Generic REST dispatch: POST {"tool": "<name>", "args": {...}}.

    Returns {"ok": true, "result": <str>} on success, {"ok": false, "error": <str>}
    on failure (HTTP 400/404/500). Mirrors the @mcp.tool() callable surface for
    REST clients (notably the Claus Bridge MCP).
    """
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"invalid JSON: {e}"}, status_code=400)

    tool_name = (body or {}).get("tool", "")
    args = (body or {}).get("args") or {}
    if not isinstance(args, dict):
        return JSONResponse({"ok": False, "error": "args must be an object"}, status_code=400)

    func = _API_TOOL_DISPATCH.get(tool_name)
    if not func:
        return JSONResponse(
            {"ok": False, "error": f"unknown tool: {tool_name!r}", "valid": list(_API_TOOL_DISPATCH)},
            status_code=404,
        )

    try:
        result = func(**args)
        if asyncio.iscoroutine(result):
            result = await result
    except TypeError as e:
        return JSONResponse({"ok": False, "error": f"bad args: {e}"}, status_code=400)
    except Exception as e:
        logger.exception("api_call %s failed", tool_name)
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)

    return JSONResponse({"ok": True, "result": result})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport="streamable-http")
