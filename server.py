"""
Eurostat + KSH + DBnomics MCP Server
======================================
Unified MCP connector for European, Hungarian, and global statistical data.
Deployable on Railway with Streamable HTTP transport.

Tools:
  - search_datasets: Search Eurostat, KSH, and/or DBnomics datasets by keyword
  - get_eurostat_data: Fetch data from Eurostat (JSON-stat API)
  - get_ksh_datasets: List/search KSH High-Value Datasets
  - get_ksh_data: Download KSH dataset as CSV
  - dbnomics_providers: List all DBnomics data providers (IMF, ECB, OECD, etc.)
  - dbnomics_search: Search for series across all DBnomics providers
  - dbnomics_series: Fetch time series data from DBnomics
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
from starlette.responses import HTMLResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("eurostat-ksh-mcp")

# ---------------------------------------------------------------------------
# Server setup
# ---------------------------------------------------------------------------
mcp = FastMCP(
    "Eurostat-KSH-DBnomics",
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

# Country name → Eurostat geo code mapping (for smarter search)
_COUNTRY_TO_GEO: dict[str, str] = {
    "austria": "AT", "belgium": "BE", "bulgaria": "BG", "croatia": "HR",
    "cyprus": "CY", "czech republic": "CZ", "czechia": "CZ",
    "denmark": "DK", "estonia": "EE", "finland": "FI", "france": "FR",
    "germany": "DE", "greece": "EL", "hungary": "HU", "ireland": "IE",
    "italy": "IT", "latvia": "LV", "lithuania": "LT", "luxembourg": "LU",
    "malta": "MT", "netherlands": "NL", "poland": "PL", "portugal": "PT",
    "romania": "RO", "slovakia": "SK", "slovenia": "SI", "spain": "ES",
    "sweden": "SE", "norway": "NO", "switzerland": "CH", "iceland": "IS",
    "turkey": "TR", "türkiye": "TR", "united kingdom": "UK", "uk": "UK",
    "eu": "EU27_2020", "euro area": "EA20", "eurozone": "EA20",
    # Hungarian names
    "magyarország": "HU", "ausztria": "AT", "németország": "DE",
    "franciaország": "FR", "olaszország": "IT", "spanyolország": "ES",
    "lengyelország": "PL", "románia": "RO", "csehország": "CZ",
    "szlovénia": "SI", "horvátország": "HR", "szlovákia": "SK",
    "bulgária": "BG", "szerbia": "RS", "svájc": "CH",
    "svédország": "SE", "norvégia": "NO", "dánia": "DK",
    "finnország": "FI", "észtország": "EE", "lettország": "LV",
    "litvánia": "LT", "hollandia": "NL", "belgium": "BE",
    "portugália": "PT", "görögország": "EL", "írország": "IE",
    "törökország": "TR",
}

# Topic synonyms: user term → additional search keywords
_TOPIC_SYNONYMS: dict[str, list[str]] = {
    "wages": ["earnings", "compensation", "remuneration", "salary", "labour cost"],
    "salary": ["earnings", "wages", "compensation", "remuneration"],
    "earnings": ["wages", "compensation", "salary"],
    "pay": ["wages", "earnings", "compensation", "salary"],
    "fizetés": ["earnings", "wages", "kereset", "bér", "compensation"],
    "bér": ["earnings", "wages", "kereset", "fizetés", "compensation"],
    "kereset": ["earnings", "wages", "bér", "fizetés"],
    "átlagfizetés": ["earnings", "wages", "mean annual", "average"],
    "átlagkereset": ["earnings", "wages", "mean annual", "average"],
    "inflation": ["hicp", "consumer price", "price index", "cpi"],
    "infláció": ["hicp", "consumer price", "price index", "cpi", "inflation"],
    "gdp": ["gross domestic product", "national accounts", "nama"],
    "unemployment": ["jobless", "labour force", "munkanélküli"],
    "munkanélküliség": ["unemployment", "labour force", "jobless"],
    "trade": ["export", "import", "balance of payments"],
    "kereskedelem": ["trade", "export", "import"],
    "population": ["demography", "inhabitants", "népesség"],
    "népesség": ["population", "demography", "inhabitants"],
    "housing": ["house price", "dwelling", "rent", "lakás"],
    "lakás": ["housing", "house price", "dwelling", "rent"],
    "energy": ["electricity", "gas", "renewable", "energia"],
    "energia": ["energy", "electricity", "gas", "renewable"],
    "poverty": ["deprivation", "social exclusion", "szegénység", "income distribution"],
    "education": ["school", "student", "graduate", "isced", "oktatás"],
}

# Well-known Eurostat datasets for common queries (dataset_code: description)
_EUROSTAT_POPULAR: dict[str, dict[str, str]] = {
    "wages": {
        "earn_ses_annual": "Structure of earnings survey - annual data",
        "earn_nt_net": "Net earnings - annual",
        "earn_mw_cur": "Minimum wages",
        "lc_lci_r2_a": "Labour cost index - annual",
        "earn_ses_pub2s": "Mean annual earnings by sex, economic activity",
    },
    "inflation": {
        "prc_hicp_manr": "HICP - monthly annual rate of change",
        "prc_hicp_aind": "HICP - annual average index",
        "prc_hicp_midx": "HICP - monthly index",
    },
    "gdp": {
        "nama_10_gdp": "GDP and main components",
        "nama_10_pc": "GDP per capita",
        "namq_10_gdp": "GDP quarterly",
    },
    "unemployment": {
        "une_rt_m": "Unemployment rate - monthly",
        "une_rt_a": "Unemployment rate - annual",
        "lfsi_emp_a": "Employment rates - annual",
    },
    "population": {
        "demo_pjan": "Population on 1 January",
        "demo_gind": "Population change",
        "demo_mlexpec": "Life expectancy",
    },
    "trade": {
        "ext_lt_maineu": "EU trade - main partners",
        "bop_c6_a": "Balance of payments - annual",
    },
    "housing": {
        "prc_hpi_a": "House price index - annual",
        "ilc_lvho02": "Housing cost overburden rate",
    },
    "energy": {
        "nrg_bal_c": "Complete energy balances",
        "nrg_pc_204": "Electricity prices - households",
    },
    "poverty": {
        "ilc_li02": "At-risk-of-poverty rate",
        "ilc_peps01n": "People at risk of poverty or social exclusion",
    },
}

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


def _expand_search_keywords(query: str) -> list[str]:
    """Expand user query with synonyms and country code mappings.

    Handles:
      - Country names → geo codes (e.g. "slovenia" adds "SI")
      - Topic synonyms (e.g. "wages" adds "earnings", "compensation")
      - Year stripping (years like "2009" are not useful for title search)
    """
    query_lower = query.lower()
    words = query_lower.split()
    expanded = set()

    for word in words:
        # Skip pure years — they don't appear in Eurostat TOC titles
        if re.match(r"^\d{4}$", word):
            continue
        expanded.add(word)

        # Country name → add geo code as extra keyword
        if word in _COUNTRY_TO_GEO:
            expanded.add(_COUNTRY_TO_GEO[word].lower())

        # Check multi-word country names (e.g. "czech republic", "united kingdom")
        for cname, code in _COUNTRY_TO_GEO.items():
            if " " in cname and cname in query_lower:
                expanded.add(code.lower())

        # Topic synonyms
        if word in _TOPIC_SYNONYMS:
            for syn in _TOPIC_SYNONYMS[word]:
                expanded.add(syn.lower())

    return list(expanded)


def _search_toc(entries: list[dict], query: str, limit: int = 20) -> list[dict]:
    """Case-insensitive keyword search with relevance scoring and synonym expansion.

    Expands the query with:
      - Country name → geo code mapping (e.g. "Slovenia" → also searches "SI")
      - Topic synonyms (e.g. "wages" → also searches "earnings", "compensation")
      - Year filtering (pure year numbers are stripped as they don't appear in titles)

    Uses OR logic with scoring: entries matching more keywords rank higher.
    """
    expanded_keywords = _expand_search_keywords(query)
    if not expanded_keywords:
        return []

    # Also get original keywords for bonus scoring
    original_words = [w.lower() for w in query.split() if not re.match(r"^\d{4}$", w)]

    scored = []
    for entry in entries:
        text = f"{entry.get('code', '')} {entry.get('title', '')}".lower()
        # Score: count how many expanded keywords match
        match_count = sum(1 for kw in expanded_keywords if kw in text)
        if match_count > 0:
            # Bonus for original (non-synonym) keyword matches
            original_match = sum(1 for kw in original_words if kw in text)
            score = match_count + original_match * 2  # original matches weigh more
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

    if not dims or not values:
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

    # Truncate if too many rows
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
    """Search for statistical datasets by keyword across Eurostat, KSH, and DBnomics.

    The search automatically expands queries with:
      - Country name → Eurostat geo code (e.g. "Slovenia" also matches "SI")
      - Topic synonyms (e.g. "wages" also matches "earnings", "compensation")
      - Hungarian country/topic names are supported

    Well-known Eurostat datasets for common topics:
      - Wages/earnings: earn_ses_annual, earn_nt_net, earn_mw_cur, lc_lci_r2_a
      - Inflation: prc_hicp_manr, prc_hicp_aind, prc_hicp_midx
      - GDP: nama_10_gdp, nama_10_pc, namq_10_gdp
      - Unemployment: une_rt_m, une_rt_a, lfsi_emp_a
      - Population: demo_pjan, demo_gind, demo_mlexpec
      - Trade: ext_lt_maineu, bop_c6_a
      - Housing: prc_hpi_a | Energy: nrg_bal_c | Poverty: ilc_li02

    TIP: For Eurostat data, first search to find the dataset code, then use
    get_eurostat_data with geo filter (e.g. geo="SI" for Slovenia) and time filters.

    Args:
        query: Search keywords (e.g. "GDP Hungary", "inflation", "wages Slovenia")
        source: Data source - "eurostat", "ksh", "dbnomics", "all", or "both" (eurostat+ksh). Default: "all"
        limit: Maximum results per source (default: 20)

    Returns:
        JSON with matching datasets including codes/IDs and titles.
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
        sources = {"eurostat", "ksh", "dbnomics"}
    else:
        sources = {source}

    results = {"eurostat": [], "ksh": [], "dbnomics": []}

    if "eurostat" in sources:
        toc = await _load_eurostat_toc()
        matches = _search_toc(toc, query, limit)
        results["eurostat"] = [
            {"code": m["code"], "title": m["title"], "source": "eurostat"}
            for m in matches
        ]
        # Inject well-known datasets for common topics if they're not already in results
        query_lower = query.lower()
        matched_codes = {m["code"] for m in matches}
        suggested = []
        for topic, datasets in _EUROSTAT_POPULAR.items():
            # Check if query relates to this topic (direct or via synonyms)
            topic_words = {topic} | set(_TOPIC_SYNONYMS.get(topic, []))
            if any(tw in query_lower for tw in topic_words):
                for code, desc in datasets.items():
                    if code not in matched_codes:
                        suggested.append({"code": code, "title": desc, "source": "eurostat", "suggested": True})
                        matched_codes.add(code)
        if suggested:
            results["eurostat_suggested"] = suggested[:10]

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
                    "tool": "get_ksh_data",
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
        dataset_code: Eurostat dataset code (e.g. "nama_10_gdp", "prc_hicp_manr", "earn_ses_annual")
        geo: Country/region filter - comma-separated ISO codes: AT, BE, BG, HR, CY, CZ, DK, EE, FI, FR, DE, EL (Greece), HU, IE, IT, LV, LT, LU, MT, NL, PL, PT, RO, SK, SI (Slovenia), ES, SE, NO, CH, UK, EU27_2020, EA20. Example: "SI" for Slovenia, "HU,DE" for Hungary+Germany.
        time: Time period filter for specific years (e.g. "2009", "2020,2021,2022")
        sinceTimePeriod: Start of time range (e.g. "2002-01", "2002"). Use with untilTimePeriod for ranges.
        untilTimePeriod: End of time range (e.g. "2008-12", "2008"). Use with sinceTimePeriod for ranges.
        filters: Additional dimension filters as "KEY=VAL&KEY2=VAL2" (e.g. "unit=CP_MEUR&na_item=B1GQ")
        lang: Language - EN, FR, or DE (default: EN)

    Returns:
        JSON with parsed data table. Use search_datasets first to find dataset codes.
    """
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
async def get_ksh_datasets(
    query: str = "",
    lang: str = "hu",
) -> str:
    """List or search KSH (Hungarian Central Statistical Office) High-Value Datasets.

    Args:
        query: Optional search keywords (searches in titles, descriptions, themes, tags)
        lang: Preferred language for titles - "hu" or "en" (default: "hu")

    Returns:
        JSON list of available KSH datasets with IDs, titles, themes and tags.
    """
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


@mcp.tool()
async def get_ksh_data(
    dataset_id: str,
    max_rows: int = 200,
) -> str:
    """Download data from a KSH High-Value Dataset.

    Args:
        dataset_id: KSH dataset UUID (get from get_ksh_datasets or search_datasets)
        max_rows: Maximum rows to return (default: 200, max: 1000)

    Returns:
        JSON with dataset metadata and CSV data parsed as rows.
        First fetches metadata.rdf to find download URLs, then downloads CSV.
    """
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
            "hint": "Check dataset_id. Use get_ksh_datasets to find valid IDs.",
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)


@mcp.tool()
async def dbnomics_providers(
    query: str = "",
) -> str:
    """List DBnomics data providers (IMF, ECB, OECD, World Bank, national offices, etc.).

    Args:
        query: Optional search keyword to filter providers (e.g. "IMF", "bank", "Hungary")

    Returns:
        JSON list of providers with codes, names, and dataset counts.
        Use provider codes with dbnomics_search and dbnomics_series.
    """
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


@mcp.tool()
async def dbnomics_search(
    query: str,
    provider: str = "",
    limit: int = 20,
) -> str:
    """Search for datasets and series across DBnomics (700M+ series from 70+ providers).

    Args:
        query: Search keywords (e.g. "GDP per capita", "consumer price index", "unemployment rate")
        provider: Optional provider code to restrict search (e.g. "IMF", "OECD", "ECB", "WB", "Eurostat")
        limit: Maximum results (default: 20, max: 50)

    Returns:
        JSON with matching datasets including provider, dataset codes, and series counts.
        Use provider_code + dataset_code with dbnomics_series to fetch actual data.
    """
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

    Args:
        provider_code: Provider code (e.g. "IMF", "ECB", "OECD", "WB", "Eurostat", "AMECO")
        dataset_code: Dataset code (e.g. "WEO:latest", "EXR", "ZUTN", "nama_10_gdp")
        series_code: Specific series code (e.g. "A.HU.NGDP_RPCH" for IMF WEO). Optional.
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
        if len(rows) >= max_rows:
            break

    return {
        "title": title,
        "columns": headers,
        "row_count": len(rows),
        "total_rows_in_file": len(lines) - data_start,
        "truncated": len(rows) >= max_rows,
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
        ara0004 — Core inflation
        ara0039 — CPI detailed monthly
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
async def yfinance_quote(
    symbol: str,
) -> str:
    """Get current quote and key stats for a financial instrument from Yahoo Finance.

    Args:
        symbol: Yahoo Finance ticker symbol. Examples:
            Stocks: "AAPL", "MSFT", "OTP.BD" (OTP Budapest), "EBS.VI" (Erste Vienna)
            CEE: .BD (Budapest), .VI (Vienna), .WA (Warsaw), .PR (Prague)
            Forex: "EURHUF=X" (EUR/HUF), "USDHUF=X", "EURUSD=X"
            Commodities: "GC=F" (gold), "CL=F" (crude oil), "BZ=F" (Brent)
            Crypto: "BTC-USD", "ETH-USD"
            Bonds: "^TNX" (US 10Y yield), "^IRX" (US 3M T-bill)
            Indices: "^BUX.BD" (BUX), "^ATX" (ATX), "^GSPC" (S&P 500), "^GDAXI" (DAX)

    Returns:
        JSON with current price, change, volume, key statistics.
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        # Detect invalid/not-found symbols
        price = info.get("regularMarketPrice") or info.get("previousClose")
        if not price and not info.get("currency"):
            return json.dumps({
                "error": f"Symbol '{symbol}' not found or has no data",
                "hint": "Check the symbol format. Examples: 'AAPL', 'OTP.BD', 'EURHUF=X', 'GC=F', '^BUX'",
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
        }
        # Remove None values
        result = {k: v for k, v in result.items() if v is not None}

        return json.dumps(result, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({
            "error": str(e),
            "hint": f"Check symbol '{symbol}'. Examples: 'AAPL', 'EURHUF=X', 'GC=F', '^BUX'",
        }, indent=2)


@mcp.tool()
async def yfinance_history(
    symbol: str,
    period: str = "1y",
    interval: str = "1d",
    start: str = "",
    end: str = "",
) -> str:
    """Get historical price data from Yahoo Finance.

    Args:
        symbol: Ticker symbol (e.g. "EURHUF=X", "^BUX", "CL=F", "AAPL")
        period: Time period - "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "max"
                Ignored if start/end provided.
        interval: Data frequency - "1d" (daily), "1wk" (weekly), "1mo" (monthly).
                  For intraday: "1m", "5m", "15m", "1h" (max 7 days).
        start: Start date "YYYY-MM-DD" (optional, overrides period)
        end: End date "YYYY-MM-DD" (optional)

    Returns:
        JSON with OHLCV price history.

    Common symbols for economists:
        Forex: EURHUF=X, USDHUF=X, EURUSD=X, USDTRY=X, USDRUB=X, USDCNY=X
        CEE indices: ^BUX.BD (BUX), ^ATX (ATX Vienna), ^GSPC (S&P 500), ^GDAXI (DAX)
        Budapest: OTP.BD, MOL.BD, RICHTER.BD, MTELEKOM.BD, 4IG.BD
        Vienna: EBS.VI (Erste), OMV.VI, RBI.VI (Raiffeisen)
        Warsaw: PKO.WA, PZU.WA, KGH.WA (KGHM)
        Prague: CEZ.PR, KOMB.PR (Komercni Banka)
        Commodities: CL=F (WTI oil), BZ=F (Brent), NG=F (natgas), GC=F (gold), ZW=F (wheat)
        Bonds: ^TNX (US 10Y yield), ^IRX (US 3M T-bill)
    """
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

    # Safe evaluation context
    safe_ns = {
        "__builtins__": {},
        "cum_inflation": cum_inflation,
        "real_value": real_value,
        "cagr": cagr,
        "convert": convert,
        "pct_change": pct_change,
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
            "hint": "Examples: '2+3', 'cum_inflation([104.9, 103.9])', 'cagr(100, 200, 10)'",
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
def mnb_current_rates(
    currencies: str = "",
) -> str:
    """Get current official MNB (Hungarian National Bank) exchange rates for HUF.

    Args:
        currencies: Comma-separated currency codes to filter (e.g. "EUR,USD,GBP").
                    Empty = all 32 active currencies. Available: EUR, USD, GBP, CHF, JPY,
                    CZK, PLN, RON, HRK, SEK, NOK, DKK, AUD, CAD, CNY, TRY, etc.

    Returns:
        JSON with official MNB HUF exchange rates (1 unit of foreign currency = X HUF).
    """
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


@mcp.tool()
def mnb_historical_rates(
    start_date: str,
    end_date: str,
    currencies: str = "EUR,USD",
) -> str:
    """Get historical MNB exchange rates for a date range.

    Args:
        start_date: Start date YYYY-MM-DD (e.g. "2024-01-01"). Data available from 1949-01-03.
        end_date: End date YYYY-MM-DD (e.g. "2024-12-31")
        currencies: Comma-separated currency codes (e.g. "EUR,USD,CHF"). Default: "EUR,USD"

    Returns:
        JSON with daily MNB rates. Note: no rates on weekends/holidays.
    """
    from datetime import date as date_type

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
# Recipe lookup — curated dataset+dimension combos for common macro queries
# ---------------------------------------------------------------------------

_MACRO_RECIPES: dict[str, dict] = {
    # ── KAMATOK / INTEREST RATES ──────────────────────────────────────────
    "policy_rate_PL": {
        "provider": "IMF",
        "dataset": "IFS",
        "dimensions": {"FREQ": "Q", "REF_AREA": "PL", "INDICATOR": "FPOLM_PA"},
        "note": "Poland monetary policy rate, quarterly (IMF IFS)",
    },
    "short_rate_HU": {
        "provider": "OECD",
        "dataset": "DP_LIVE",
        "dimensions": {"LOCATION": "HUN", "INDICATOR": "STINT", "FREQUENCY": "Q"},
        "note": "Hungary short-term interest rate, quarterly (OECD)",
    },
    "short_rate_PL": {
        "provider": "OECD",
        "dataset": "DP_LIVE",
        "dimensions": {"LOCATION": "POL", "INDICATOR": "STINT", "FREQUENCY": "Q"},
        "note": "Poland short-term interest rate, quarterly (OECD)",
    },
    "short_rate_EA": {
        "provider": "OECD",
        "dataset": "DP_LIVE",
        "dimensions": {"LOCATION": "EA19", "INDICATOR": "STINT", "FREQUENCY": "Q"},
        "note": "Euro area short-term interest rate, quarterly (OECD)",
    },
    "ecb_main_refi": {
        "provider": "ECB",
        "dataset": "FM",
        "series_code": "B.U2.EUR.4F.KR.MRR_FR.LEV",
        "note": "ECB main refinancing operations rate",
    },
    # ── BÉREK / WAGES ─────────────────────────────────────────────────────
    "wages_SI_gross": {
        "provider": "Eurostat",
        "dataset": "earn_nt_net",
        "dimensions": {"geo": "SI", "currency": "EUR", "estruct": "GRS_P1_NCH_AW100"},
        "note": "Slovenia gross annual earnings, single earner 100% AW, EUR",
    },
    "wages_EE_gross": {
        "provider": "Eurostat",
        "dataset": "earn_nt_net",
        "dimensions": {"geo": "EE", "currency": "EUR", "estruct": "GRS_P1_NCH_AW100"},
        "note": "Estonia gross annual earnings, single earner 100% AW, EUR",
    },
    "wages_SI_net": {
        "provider": "Eurostat",
        "dataset": "earn_nt_net",
        "dimensions": {"geo": "SI", "currency": "EUR", "estruct": "NET_P1_NCH_AW100"},
        "note": "Slovenia net annual earnings, single earner 100% AW, EUR",
    },
    "wages_EE_net": {
        "provider": "Eurostat",
        "dataset": "earn_nt_net",
        "dimensions": {"geo": "EE", "currency": "EUR", "estruct": "NET_P1_NCH_AW100"},
        "note": "Estonia net annual earnings, single earner 100% AW, EUR",
    },
    "wages_pps_SI": {
        "provider": "Eurostat",
        "dataset": "earn_nt_netft",
        "dimensions": {"geo": "SI", "estruct": "VAL_A", "currency": "PPS"},
        "note": "Slovenia annual net earnings in PPS (purchasing power standard)",
    },
    "wages_pps_EE": {
        "provider": "Eurostat",
        "dataset": "earn_nt_netft",
        "dimensions": {"geo": "EE", "estruct": "VAL_A", "currency": "PPS"},
        "note": "Estonia annual net earnings in PPS (purchasing power standard)",
    },
    "wages_HU_sector": {
        "provider": "KSH",
        "dataset": "mun0183",
        "note": "Hungary gross average earnings by sector (KSH STADAT). Use get_ksh_stadat(table_code='mun0183').",
    },
    "wages_HU_monthly": {
        "provider": "KSH",
        "dataset": "mun0143",
        "note": "Hungary monthly gross average earnings (KSH STADAT). Use get_ksh_stadat(table_code='mun0143').",
    },
    # ── INFLÁCIÓ / INFLATION ──────────────────────────────────────────────
    "cpi_HU": {
        "provider": "KSH",
        "dataset": "ara0001",
        "note": "Hungary consumer price index (KSH STADAT). Use get_ksh_stadat(table_code='ara0001').",
    },
    "hicp_EA_index": {
        "provider": "Eurostat",
        "dataset": "prc_hicp_aind",
        "dimensions": {"geo": "EA", "coicop": "CP00", "unit": "INX_A_AVG"},
        "note": "Euro area HICP annual average index (all items, 2015=100)",
    },
    "hicp_annual_rate": {
        "provider": "Eurostat",
        "dataset": "prc_hicp_manr",
        "dimensions": {"coicop": "CP00", "unit": "RCH_A"},
        "note": "HICP monthly annual rate of change — add geo param (e.g. geo=HU,SI) for specific countries",
    },
    # ── ÁRFOLYAM / EXCHANGE RATES ─────────────────────────────────────────
    "eur_huf_current": {
        "provider": "MNB",
        "tool": "mnb_current_rates",
        "call_example": "mnb_current_rates(currencies='EUR')",
        "note": "Current EUR/HUF official MNB rate",
    },
    "eur_huf_historical": {
        "provider": "MNB",
        "tool": "mnb_historical_rates",
        "call_example": "mnb_historical_rates(start_date='2020-01-01', end_date='2024-12-31', currencies='EUR')",
        "note": "Historical EUR/HUF rates from MNB (available since 1949)",
    },
    # ── GDP ────────────────────────────────────────────────────────────────
    "gdp_HU": {
        "provider": "KSH",
        "dataset": "gdp0001",
        "note": "Hungary GDP data (KSH STADAT). Use get_ksh_stadat(table_code='gdp0001').",
    },
    "gdp_growth_EU": {
        "provider": "Eurostat",
        "dataset": "namq_10_gdp",
        "dimensions": {"na_item": "B1GQ", "unit": "CLV_PCH_PRE"},
        "note": "EU GDP growth rate, quarterly. Also try tec00115 for annual overview. Add geo param for country filter.",
    },
}

# Search keywords mapped to recipe keys — lowercase, includes synonyms
_RECIPE_SEARCH_TAGS: dict[str, list[str]] = {
    "policy_rate_PL": ["policy rate", "poland", "lengyel", "lengyelország", "kamat", "interest rate", "jegybanki", "monetary policy", "imf", "pl"],
    "short_rate_HU": ["short rate", "hungary", "magyar", "magyarország", "kamat", "interest rate", "rövid", "oecd", "hu"],
    "short_rate_PL": ["short rate", "poland", "lengyel", "lengyelország", "kamat", "interest rate", "rövid", "oecd", "pl"],
    "short_rate_EA": ["short rate", "euro area", "eurozone", "eurózóna", "ea", "kamat", "interest rate", "rövid", "oecd"],
    "ecb_main_refi": ["ecb", "refinancing", "refi", "main rate", "irányadó", "kamat", "interest rate", "european central bank"],
    "wages_SI_gross": ["wages", "salary", "earnings", "gross", "bruttó", "bér", "fizetés", "kereset", "slovenia", "szlovénia", "si"],
    "wages_EE_gross": ["wages", "salary", "earnings", "gross", "bruttó", "bér", "fizetés", "kereset", "estonia", "észtország", "ee"],
    "wages_SI_net": ["wages", "salary", "earnings", "net", "nettó", "bér", "fizetés", "kereset", "slovenia", "szlovénia", "si"],
    "wages_EE_net": ["wages", "salary", "earnings", "net", "nettó", "bér", "fizetés", "kereset", "estonia", "észtország", "ee"],
    "wages_pps_SI": ["wages", "salary", "earnings", "pps", "purchasing power", "vásárlóerő", "bér", "fizetés", "slovenia", "szlovénia", "si"],
    "wages_pps_EE": ["wages", "salary", "earnings", "pps", "purchasing power", "vásárlóerő", "bér", "fizetés", "estonia", "észtország", "ee"],
    "wages_HU_sector": ["wages", "salary", "earnings", "sector", "ágazat", "bér", "fizetés", "kereset", "hungary", "magyar", "hu"],
    "wages_HU_monthly": ["wages", "salary", "earnings", "monthly", "havi", "bér", "fizetés", "kereset", "hungary", "magyar", "hu", "átlag"],
    "cpi_HU": ["cpi", "consumer price", "inflation", "infláció", "fogyasztói ár", "hungary", "magyar", "hu"],
    "hicp_EA_index": ["hicp", "harmonized", "harmonizált", "price index", "inflation", "infláció", "euro area", "ea", "index"],
    "hicp_annual_rate": ["hicp", "inflation", "infláció", "annual rate", "éves", "ráta", "price change"],
    "eur_huf_current": ["eur", "huf", "forint", "euro", "exchange rate", "árfolyam", "current", "mai", "aktuális", "mnb"],
    "eur_huf_historical": ["eur", "huf", "forint", "euro", "exchange rate", "árfolyam", "historical", "történeti", "múltbeli", "mnb"],
    "gdp_HU": ["gdp", "gross domestic product", "bruttó hazai termék", "hungary", "magyar", "hu"],
    "gdp_growth_EU": ["gdp", "growth", "növekedés", "eu", "europe", "európa", "quarterly", "negyedéves"],
}


@mcp.tool()
def get_recipe(
    topic: str,
) -> str:
    """Look up curated data recipes for common macroeconomic queries.

    Returns the exact provider, dataset code, and dimension filters needed
    to fetch data — no trial and error required.

    Covers: interest rates, wages/earnings, inflation/CPI/HICP, exchange rates, GDP.
    Searchable in English and Hungarian.

    Args:
        topic: Search phrase (e.g. "kamat lengyelország", "wages slovenia",
               "inflation hungary", "ecb rate", "gdp eu", "árfolyam eur huf",
               "bér szlovénia", "hicp euro area")

    Returns:
        JSON with provider, dataset, dimensions/series_code, and usage notes.
        If no match: returns error with hint to use search_datasets or dbnomics_search.
    """
    if not topic or not topic.strip():
        return json.dumps({
            "error": "Please provide a search topic",
            "hint": "Examples: 'wages slovenia', 'kamat lengyelország', 'inflation hungary', 'gdp eu'",
            "available_topics": "interest rates, wages, inflation, exchange rates, GDP",
        }, ensure_ascii=False, indent=2)

    query_lower = topic.lower().strip()
    query_words = query_lower.split()

    # Score each recipe by keyword overlap
    scored: list[tuple[int, str, dict]] = []
    for key, tags in _RECIPE_SEARCH_TAGS.items():
        score = 0
        for qw in query_words:
            for tag in tags:
                if qw in tag or tag in query_lower:
                    score += 1
                    break
        # Bonus: exact recipe key match
        if query_lower == key or query_lower.replace(" ", "_") == key:
            score += 100
        if score > 0:
            scored.append((score, key, _MACRO_RECIPES[key]))

    if not scored:
        return json.dumps({
            "error": "No recipe found",
            "query": topic,
            "hint": "Use search_datasets() or dbnomics_search() for broader search",
            "available_recipes": sorted(_MACRO_RECIPES.keys()),
        }, ensure_ascii=False, indent=2)

    # Sort by score descending
    scored.sort(key=lambda x: -x[0])

    # Return top matches
    matches = []
    for score, key, recipe in scored[:5]:
        entry = {"recipe_key": key, **recipe}
        matches.append(entry)

    if len(matches) == 1:
        return json.dumps(matches[0], ensure_ascii=False, indent=2)

    return json.dumps({
        "query": topic,
        "matches": len(matches),
        "recipes": matches,
        "tip": "Use the recipe_key as topic for exact match, e.g. get_recipe('wages_SI_gross')",
    }, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Landing page
# ---------------------------------------------------------------------------
LANDING_HTML = """<!DOCTYPE html>
<html lang="hu">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Makronóm — Statisztikai Adatok MCP</title>
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
    background: linear-gradient(135deg, var(--primary), #7cd22d, var(--accent-blue));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    margin-bottom: 0.6rem;
  }
  .hero .sub { font-size: 1.05rem; color: var(--text-dim); line-height: 1.7; font-weight: 300; }
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
  <h1>Statisztikai Adatok MCP a Makronóm Intézet Kutatóinak</h1>
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
    <tr><td>search_datasets</td><td>Keresés Eurostat, KSH és DBnomics adatkészletek közt</td></tr>
    <tr><td>get_eurostat_data</td><td>Eurostat adatlekérés (GDP, infláció, munkanélküliség…)</td></tr>
    <tr><td>get_ksh_stadat</td><td>KSH STADAT táblák — magyar idősorok (árak, bérek, GDP…)</td></tr>
    <tr><td>get_ksh_data</td><td>KSH High-Value Datasets letöltése</td></tr>
    <tr><td>dbnomics_search</td><td>Keresés 700M+ adatsor közt (IMF, ECB, OECD…)</td></tr>
    <tr><td>dbnomics_series</td><td>Idősor lekérése DBnomics-ból</td></tr>
    <tr><td>dbnomics_providers</td><td>DBnomics adatszolgáltatók listája</td></tr>
    <tr><td>yfinance_quote</td><td>Aktuális árfolyam (részvény, deviza, áru, index, BUX)</td></tr>
    <tr><td>yfinance_history</td><td>Historikus árfolyamadatok (napi/heti/havi OHLCV)</td></tr>
    <tr><td>mnb_current_rates</td><td>Hivatalos MNB árfolyamok (HUF, 32 deviza)</td></tr>
    <tr><td>mnb_historical_rates</td><td>MNB historikus árfolyamok (1949-től)</td></tr>
    <tr><td>calculate</td><td>Gazdasági kalkulátor (infláció, CAGR, reálérték, konverzió)</td></tr>
    <tr><td>get_recipe</td><td>Makro adatlekérési receptek — kamatok, bérek, infláció, GDP, árfolyam</td></tr>
  </table>
</div>

<footer>Makronóm Intézet</footer>
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


@mcp.custom_route("/", methods=["GET"])
async def landing_page(request):
    # Trigger background scan on first page visit if needed
    global _scan_scheduled
    if _scan_scheduled and not _ksh_scan_running:
        _scan_scheduled = False
        asyncio.create_task(_scan_ksh_stadat_background())
    return HTMLResponse(LANDING_HTML)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport="streamable-http")
