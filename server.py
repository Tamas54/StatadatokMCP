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
  - get_recipe: Look up pre-built query recipes for common macroeconomic data
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

    Args:
        query: Search keywords (e.g. "GDP Hungary", "inflation", "unemployment")
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
        dataset_code: Eurostat dataset code (e.g. "nama_10_gdp", "prc_hicp_manr")
        geo: Country/region filter - comma-separated codes (e.g. "HU,DE,EU27_2020")
        time: Time period filter for specific years (e.g. "2023", "2020,2021,2022")
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
    {"id": "eur_huf_current", "keywords": ["árfolyam", "exchange", "eur", "huf", "forint", "aktuális", "current", "mnb", "mai"], "provider": "MNB", "tool": "mnb_current_rates", "dimensions": {"currencies": "EUR"}, "note": "MNB current official EUR/HUF rate — use mnb_current_rates(currencies='EUR')"},
    {"id": "eur_huf_historical", "keywords": ["árfolyam", "exchange", "eur", "huf", "forint", "historikus", "historical", "mnb", "múlt"], "provider": "MNB", "tool": "mnb_historical_rates", "dimensions": {"currencies": "EUR"}, "note": "MNB historical EUR/HUF rates — use mnb_historical_rates(start_date, end_date, currencies='EUR')"},
    # --- GDP ---
    {"id": "gdp_HU", "keywords": ["gdp", "magyarország", "hungary", "ksh", "bruttó hazai termék"], "provider": "KSH", "dataset": "gdp0001", "tool": "get_ksh_stadat", "note": "KSH STADAT gdp0001 — Hungary GDP value and volume change"},
    {"id": "gdp_growth_EU", "keywords": ["gdp", "növekedés", "growth", "eu", "európa", "europe", "eurostat"], "provider": "Eurostat", "dataset": "namq_10_gdp", "dimensions": {"unit": "CLV_PCH_PRE", "s_adj": "SCA", "na_item": "B1GQ"}, "note": "Eurostat namq_10_gdp — EU GDP growth rate (quarterly, seasonally adjusted). Alt: tec00115 for annual overview."},
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
        return  # Already known

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


@mcp.tool()
def get_recipe(topic: str) -> str:
    """Look up pre-built query recipes for common macroeconomic data requests.

    Returns the correct provider, dataset, dimensions, and series codes so you
    don't have to guess or search. The recipe book grows automatically as users
    discover new data sources via dbnomics_series. Supports HU/EN keyword matching.

    Args:
        topic: Search query, e.g. "kamat lengyelország", "wages slovenia",
               "hicp inflation", "gdp hungary", "eur huf", "cpi magyarország"

    Returns:
        JSON with matching recipes (provider, dataset, dimensions, tool, note, stats).
        If no match: error + hint to use search_datasets or dbnomics_search.
    """
    from datetime import date as date_cls
    query_words = topic.lower().split()
    if not query_words:
        return json.dumps({"error": "empty query", "hint": "provide a topic like 'wages slovenia' or 'kamat lengyelország'"})

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

    if not scored:
        return json.dumps({
            "error": "no recipe found",
            "query": topic,
            "hint": "use search_datasets or dbnomics_search to find the right dataset. "
                    "Successful dbnomics_series calls auto-save as new recipes!",
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


@mcp.tool()
def add_recipe(
    id: str,
    provider: str,
    dataset: str,
    note: str,
    keywords: str = "",
    dimensions: str = "",
    series_code: str = "",
    tool: str = "",
) -> str:
    """Add a new recipe to the self-learning recipe book.

    Recipes are persistent — they survive server restarts. If the same
    provider+dataset+dimensions combo already exists, keywords are merged.

    Args:
        id: Unique recipe ID (e.g. "short_rate_CZ", "hicp_DE")
        provider: Data provider (e.g. "OECD", "Eurostat", "IMF", "ECB", "KSH")
        dataset: Dataset code (e.g. "DP_LIVE", "prc_hicp_manr", "IFS")
        note: Human-readable description of what this recipe returns
        keywords: Comma-separated search keywords (e.g. "czech,cseh,kamat,rate,CZE,interest")
        dimensions: JSON string of dimension filters (e.g. '{"LOCATION":"CZE","INDICATOR":"STINT"}')
        series_code: Specific series code if applicable (e.g. "B.U2.EUR.4F.KR.MRR_FR.LEV")
        tool: MCP tool to use (e.g. "get_ksh_stadat", "mnb_current_rates"). Empty = dbnomics_series.

    Returns:
        JSON confirmation with the saved recipe.
    """
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

    # Fetch series info + observations in parallel
    try:
        info_resp, obs_resp = await asyncio.gather(
            client.get(f"{_FRED_BASE}/series", params={
                "series_id": series_id,
                "api_key": _FRED_API_KEY,
                "file_type": "json",
            }),
            client.get(f"{_FRED_BASE}/series/observations", params=params),
        )

        # Parse series metadata
        meta = {}
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


@mcp.tool()
async def get_oecd_cli(
    country: str = "HUN",
    periods: int = 12,
) -> str:
    """Get the OECD Composite Leading Indicator (CLI) for a country.

    The CLI predicts turning points in business cycles 6-9 months ahead.
    Values above 100 = expansion, below 100 = contraction.

    Args:
        country: Country code — 2-letter (HU, DE, US) or 3-letter (HUN, DEU, USA).
                 Available: DE, FR, IT, ES, NL, BE, AT, PL, CZ, HU, SE, DK, FI, PT, GR, IE,
                 US, GB, JP, CN, CA, AU, KR, MX, BR, IN, RU, ZA, TR, CH, NO
        periods: Number of monthly observations (default: 12)

    Returns:
        JSON with CLI value, trend direction, momentum (expansion/contraction), and history.

    Interpretation:
        CLI > 100 + trending up → strong expansion ahead
        CLI > 100 + trending down → expansion peaking, slowdown coming
        CLI < 100 + trending down → contraction deepening
        CLI < 100 + trending up → contraction bottoming, recovery coming
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

        latest = values[-1]

        # Trend: 3-month change
        trend = None
        trend_direction = "unknown"
        if len(values) >= 4:
            trend = round(values[-1] - values[-4], 3)
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
            "latest_period": periods_list[-1] if periods_list else None,
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


@mcp.tool()
def forecast(
    country: str,
    indicator: str = "gdp",
    year: int = 2026,
    quarter: int = 0,
) -> str:
    """Get macroeconomic forecasts — GDP growth, inflation, unemployment.

    Ensemble model combining SAJÁT (Phillips Curve + Okun's Law), IMF WEO,
    OECD Composite Leading Indicator, and FRED data. Supports 52 countries,
    annual and quarterly forecasts, with 3 scenarios.

    Args:
        country: ISO 2-letter country code (e.g. "HU", "DE", "US", "PL", "FR")
                 Supported: V4 (HU,PL,CZ,SK), DACH (DE,AT,CH), Western EU (FR,IT,ES,NL,BE,PT,IE),
                 Nordics (SE,DK,FI,NO), Balkans (RO,BG,HR,SI), Baltics (EE,LV,LT),
                 Global (US,GB,JP,CN,CA,AU,KR,IN,BR,MX,TR,ZA)
        indicator: "gdp" (growth %), "inflation" (CPI %), or "unemployment" (rate %).
        year: Target year for forecast (default: 2026)
        quarter: Quarter 1-4 for quarterly forecast, 0 for annual (default: 0).
                 Quarterly forecasts are our EXCLUSIVE capability — most sources only have annual!

    Returns:
        JSON with ensemble forecast, individual source values, weights, confidence,
        3 scenarios (pessimistic/realistic/optimistic), and recession probability.

    Examples:
        forecast("HU", "gdp", 2026) → Hungary GDP growth forecast for 2026
        forecast("DE", "inflation", 2026, 2) → Germany Q2 2026 inflation forecast
        forecast("US", "unemployment", 2026) → US unemployment rate forecast
    """
    uf = _get_forecaster()
    if uf is None:
        return json.dumps({
            "error": "Forecaster not available",
            "detail": _forecaster_error or "UltimateForecaster failed to initialize",
            "hint": "Check server logs for missing dependencies (pandas, numpy, requests)",
        }, indent=2)

    indicator = indicator.lower().strip()
    if indicator not in ("gdp", "inflation", "unemployment"):
        return json.dumps({
            "error": f"Unknown indicator: '{indicator}'",
            "hint": "Use 'gdp', 'inflation', or 'unemployment'",
        })

    country = country.strip().upper()
    q = quarter if quarter and 1 <= quarter <= 4 else None

    try:
        # Fetch leading indicators first (needed for composite score)
        uf.fetch_all_indicators()

        result = uf.get_ultimate_forecast(country, indicator, year, quarter=q)

        # Clean up for JSON serialization (numpy types → Python native)
        def _clean(obj):
            if isinstance(obj, (float,)):
                if obj != obj:  # NaN check
                    return None
                return round(obj, 2)
            if hasattr(obj, 'item'):  # numpy scalar
                return round(float(obj), 2)
            if isinstance(obj, dict):
                return {k: _clean(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple)):
                return [_clean(v) for v in obj]
            return obj

        cleaned = _clean(result)

        # Add recession probability
        try:
            composite = uf.calculate_composite_score(country)
            cleaned["recession_probability"] = round(composite.get("recession_probability", 0), 1)
            cleaned["composite_score"] = round(composite.get("composite_score", 0), 1)
            cleaned["gdp_signal"] = composite.get("gdp_growth_signal", "unknown")
        except Exception:
            pass

        cleaned["source_description"] = (
            "Ensemble: SAJÁT (Phillips Curve + Okun's Law) + IMF WEO + "
            "OECD CLI + FRED. Weights based on backtesting accuracy."
        )

        return json.dumps(cleaned, ensure_ascii=False, indent=2)

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
    <tr><td>search_datasets</td><td>Keresés Eurostat, KSH és DBnomics közt — szinonimákkal (ország + téma)</td></tr>
    <tr><td>get_recipe</td><td>Öntanuló receptkönyv — kész lekérési sablonok, automatikusan bővül</td></tr>
    <tr><td>add_recipe</td><td>Recept hozzáadása a receptkönyvhöz (bárki hívhatja)</td></tr>
    <tr><td>get_eurostat_data</td><td>Eurostat adatlekérés (GDP, infláció, munkanélküliség…)</td></tr>
    <tr><td>get_ksh_stadat</td><td>KSH STADAT táblák — magyar idősorok (árak, bérek, GDP…)</td></tr>
    <tr><td>get_ksh_data</td><td>KSH High-Value Datasets letöltése</td></tr>
    <tr><td>get_fred_data</td><td>FRED — 800K+ US gazdasági idősor (kamatok, infláció, GDP, munkaerő…)</td></tr>
    <tr><td>dbnomics_search</td><td>Keresés 700M+ adatsor közt (IMF, ECB, OECD…)</td></tr>
    <tr><td>dbnomics_series</td><td>Idősor lekérése DBnomics-ból — sikeres lekérések receptté válnak</td></tr>
    <tr><td>dbnomics_providers</td><td>DBnomics adatszolgáltatók listája</td></tr>
    <tr><td>get_oecd_cli</td><td>OECD Composite Leading Indicator — konjunktúra-előrejelzés 30+ országra</td></tr>
    <tr><td>forecast</td><td>Makrogazdasági prognózis — GDP, infláció, munkanélküliség (52 ország, negyedéves)</td></tr>
    <tr><td>yfinance_quote</td><td>Aktuális árfolyam (részvény, deviza, áru, index, BUX)</td></tr>
    <tr><td>yfinance_history</td><td>Historikus árfolyamadatok (napi/heti/havi OHLCV)</td></tr>
    <tr><td>mnb_current_rates</td><td>Hivatalos MNB árfolyamok (HUF, 32 deviza)</td></tr>
    <tr><td>mnb_historical_rates</td><td>MNB historikus árfolyamok (1949-től)</td></tr>
    <tr><td>get_economic_calendar</td><td>Gazdasági naptár — közelgő adatközlések (FRED, ECB, Eurostat)</td></tr>
    <tr><td>calculate</td><td>Gazdasági kalkulátor (infláció, CAGR, reálérték, konverzió)</td></tr>
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
