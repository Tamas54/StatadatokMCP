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

import csv
import io
import os
import json
import logging
import re
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


def _search_toc(entries: list[dict], query: str, limit: int = 20) -> list[dict]:
    """Case-insensitive keyword search with relevance scoring.

    Uses OR logic with scoring: entries matching more keywords rank higher.
    Entries matching ALL keywords come first, then partial matches.
    """
    query_lower = query.lower()
    keywords = query_lower.split()
    if not keywords:
        return []

    scored = []
    for entry in entries:
        text = f"{entry.get('code', '')} {entry.get('title', '')}".lower()
        score = sum(1 for kw in keywords if kw in text)
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
        query_lower = query.lower()
        keywords = query_lower.split()
        ksh_scored = []

        # Search STADAT catalog with scoring (OR logic)
        for code, title in KSH_STADAT_CATALOG.items():
            text = f"{code} {title}".lower()
            score = sum(1 for kw in keywords if kw in text)
            if score > 0:
                ksh_scored.append((score, {
                    "code": code,
                    "title": title,
                    "tool": "get_ksh_stadat",
                    "source": "ksh_stadat",
                }))

        # Also search HVD datasets
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

        # Sort by score and take top results
        ksh_scored.sort(key=lambda x: -x[0])
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

# Curated catalog of STADAT tables — updated 2026-03-21 via KSH deep research
# NOTE: KSH reorganized prefixes: kul→kkr, ksk→bel, pen→gov, wages ber→mun
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
    # --- Wages & earnings (mun prefix — KSH merged wages into mun!) ---
    "mun0002": "Az átlagkeresetek alakulása (éves)",
    "mun0003": "A nettó átlagkeresetek alakulása (éves)",
    "mun0004": "A reálkereset alakulása",
    "mun0025": "Teljes munkaidőben alkalmazásban állók bruttó átlagkeresete",
    "mun0026": "Teljes munkaidőben alkalmazásban állók nettó átlagkeresete",
    "mun0030": "Átlagkeresetek nemzetgazdasági áganként (havi)",
    "mun0039": "Főbb kereseti adatok a munkáltatók teljes körénél",
    "mun0044": "Közfoglalkoztatottak létszáma és átlagkeresete",
    "mun0052": "Átlagkeresetek foglalkozások (FEOR) szerint",
    "mun0058": "A bruttó keresetek mediánértéke",
    # --- Labor market (mun) ---
    "mun0001": "A munkaerőpiac legfontosabb éves mutatói",
    "mun0005": "Foglalkoztatottak száma korcsoportok szerint",
    "mun0006": "Foglalkoztatási ráta nemenként és korcsoportonként",
    "mun0007": "Munkanélküliek száma és munkanélküliségi ráta",
    "mun0008": "Gazdaságilag aktívak legmagasabb iskolai végzettség szerint",
    "mun0009": "Gazdaságilag inaktívak száma és összetétele",
    "mun0012": "Munkanélküliségi ráta vármegyénként (negyedéves)",
    "mun0016": "A munkanélküliség időtartama",
    "mun0021": "Foglalkoztatottak száma nemzetgazdasági áganként",
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
    # --- Foreign trade (kkr — was kul!) ---
    "kkr0001": "Külkereskedelmi termékforgalom összefoglaló adatai",
    "kkr0002": "Külkereskedelmi mérleg alakulása",
    "kkr0003": "Termékforgalom országcsoportok szerint",
    "kkr0004": "Termékforgalom főbb árucsoportok (SITC) szerint",
    "kkr0005": "Szolgáltatás-külkereskedelmi forgalom adatai",
    "kkr0008": "Külkereskedelmi árindexek és cserearány-mutató",
    "kkr0011": "Külkereskedelmi forgalom (havi)",
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
    # --- Government finance (gov — was pen!) ---
    "gov0001": "A kormányzati szektor egyenlege és adóssága",
    "gov0002": "A kormányzati szektor bevételei és kiadásai",
    "gov0003": "Az államadósság alakulása a GDP százalékában",
    "gov0004": "Adó- és társadalombiztosítási bevételek",
    "gov0011": "Kormányzati pénzügyek (negyedéves)",
    # --- Tourism (tur) ---
    "tur0001": "Turisztikai szálláshelyek főbb adatai",
    "tur0002": "Vendégéjszakák száma és vendégforgalom (havi)",
    "tur0003": "Szálláshelyek bevételei és kihasználtsága",
    "tur0004": "A magyar lakosság utazási szokásai",
    "tur0005": "Turisztikai kiadások és bevételek mérlege",
    "tur0006": "Vendéglátóhelyek forgalma és száma",
    "tur0012": "Vendégéjszakák száma vármegyénként",
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

    # Detect title lines vs header vs data.
    # Title line: "1.1.1.1. Title here;;;;;;;;" (1-2 non-empty, rest empty)
    # Header line: "Év;Mutató1;Mutató2;..." (many non-empty)
    # Multi-row headers: some tables have 2-3 header rows before data starts
    title = ""
    header_idx = 0

    # Find where data starts: first line where col 0 looks like a year or numeric
    def _looks_like_data(line_parts):
        first = line_parts[0].strip().strip('"')
        if re.match(r'^\d{4}$', first):
            return True
        # Also check if it's a section label (like "Együtt", "Férfi") — still data
        if len(line_parts) > 2:
            numeric_count = sum(1 for p in line_parts[1:4] if p.strip().strip('"').replace(",", "").replace(" ", "").replace("-", "").replace(".", "").isdigit())
            if numeric_count >= 1:
                return True
        return False

    # Scan forward to find header and data start
    data_start = len(lines)
    for i in range(min(len(lines), 10)):
        parts_i = [p.strip() for p in lines[i].split(";")]
        if _looks_like_data(parts_i):
            data_start = i
            break

    # Title: first line if it has few non-empty fields
    parts0 = [p.strip() for p in lines[0].split(";")]
    nonempty0 = sum(1 for p in parts0 if p)
    if nonempty0 <= 2 and data_start > 1:
        title = parts0[0].strip('"')
        header_idx = 1
    else:
        header_idx = 0

    # Merge multi-row headers (from header_idx to data_start-1)
    header_parts = [p.strip().strip('"') for p in lines[header_idx].split(";")]
    for extra_row in range(header_idx + 1, data_start):
        extra_parts = [p.strip().strip('"') for p in lines[extra_row].split(";")]
        for j in range(min(len(header_parts), len(extra_parts))):
            if extra_parts[j]:
                if header_parts[j]:
                    header_parts[j] += " " + extra_parts[j]
                else:
                    header_parts[j] = extra_parts[j]

    headers = header_parts
    while headers and not headers[-1]:
        headers.pop()

    rows = []
    for line in lines[data_start:]:
        if not line.strip():
            continue
        values = line.split(";")
        row = {}
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
        mun0002 — Average wages (annual)
        mun0004 — Real wages
        mun0007 — Unemployment rate
        mun0030 — Wages by sector (monthly)
        ipa0001 — Industrial production
        epi0001 — Construction output
        bel0001 — Retail trade (was ksk!)
        kkr0001 — Foreign trade (was kul!)
        nep0001 — Population and vital statistics
        gov0003 — Government debt % GDP (was pen!)
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


@mcp.custom_route("/", methods=["GET"])
async def landing_page(request):
    return HTMLResponse(LANDING_HTML)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport="streamable-http")
