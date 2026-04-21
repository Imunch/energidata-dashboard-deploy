#!/usr/bin/env python3
"""
Energi Data Service — Enhanced Data Pipeline
Fetches dataset metadata, enriches with sample data & quality metrics,
and updates the dashboard HTML with live data.
Uses only stdlib (no pip dependencies).
"""

import json
import os
import re
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
import time

# Resolve paths relative to repo root (script may run from any cwd)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)  # deploy/ is one level down from repo root

API_BASE = "https://api.energidataservice.dk"
API_LIST_ENDPOINT = f"{API_BASE}/meta/dataset/list"
DASHBOARD_FILE = os.path.join(REPO_ROOT, "index.html")
ENRICHED_JSON_FILE = os.path.join(REPO_ROOT, "energidata_enriched.json")
RATE_LIMIT_DELAY = 0.5  # seconds between API calls


class PipelineLogger:
    """Simple logging with timestamps."""
    def __init__(self):
        self.start = datetime.now(timezone.utc)

    def log(self, level, msg):
        elapsed = (datetime.now(timezone.utc) - self.start).total_seconds()
        print(f"[{level:5s}] {msg}", file=sys.stderr)

    def info(self, msg):
        self.log("INFO", msg)

    def warn(self, msg):
        self.log("WARN", msg)

    def error(self, msg):
        self.log("ERROR", msg)

    def success(self, msg):
        self.log("OK", msg)


logger = PipelineLogger()


def fetch_with_retry(url, max_retries=2, timeout=30):
    """Fetch URL with basic retry logic."""
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "EnerginetDataPipeline/2.0"
                }
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warn(f"Fetch failed (attempt {attempt + 1}/{max_retries}), retrying in {wait}s: {e}")
                time.sleep(wait)
            else:
                logger.error(f"Failed to fetch {url}: {e}")
                return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from {url}: {e}")
            return None
    return None


def fetch_dataset_metadata():
    """Fetch all dataset metadata from API."""
    logger.info(f"Fetching datasets from {API_LIST_ENDPOINT}")
    data = fetch_with_retry(API_LIST_ENDPOINT, timeout=30)

    if not data:
        logger.error("Failed to fetch dataset list")
        return []

    # Handle different response formats
    if isinstance(data, dict):
        records = data.get("records") or data.get("data") or data.get("datasets") or []
    elif isinstance(data, list):
        records = data
    else:
        logger.error(f"Unexpected response type: {type(data)}")
        return []

    logger.success(f"Fetched {len(records)} datasets")
    return records


def fetch_dataset_sample(dataset_name, limit=5):
    """Fetch sample rows for a dataset."""
    time.sleep(RATE_LIMIT_DELAY)
    url = f"{API_BASE}/dataset/{dataset_name}?limit={limit}"
    try:
        data = fetch_with_retry(url, timeout=15)
        if data and isinstance(data, dict):
            return {
                "total": data.get("total") or len(data.get("records", [])),
                "records": data.get("records", [])
            }
    except Exception as e:
        logger.warn(f"Failed to fetch sample for {dataset_name}: {e}")
    return {"total": None, "records": []}


def normalize_status(raw_status):
    """Normalize API status values."""
    s = (raw_status or "").strip().lower()
    if s in ("active", "online", "available"):
        return "Active"
    elif s in ("discontinued", "offline", "deprecated", "removed"):
        return "Discontinued"
    elif s in ("under construction", "planned", "beta", "preview", "draft"):
        return "Under Construction"
    return "Active"


def normalize_resolution(raw_res):
    """Normalize resolution values."""
    if not raw_res:
        return "Unknown"
    r = raw_res.strip()
    return r[0].upper() + r[1:] if r else "Unknown"


def normalize_geo(raw_geo):
    """Normalize geography values."""
    if not raw_geo:
        return "National"
    return raw_geo.strip()


def get_organization(name):
    """Infer organization from dataset name."""
    n = name.lower()
    if "storage" in n:
        return "Gas Storage Denmark"
    if "gas" in n:
        return "TSO Gas"
    if "dso" in n or "grid capacity map dso" in n:
        return "DSO Electricity"
    return "TSO Electricity"


def compute_quality_metrics(rows):
    """Compute quality metrics from sample data."""
    if not rows:
        return {
            "completeness": 0,
            "freshness": 90,
            "fieldCount": 0
        }

    field_count = len(rows[0]) if isinstance(rows[0], dict) else 0
    total_fields = len(rows) * field_count
    null_fields = 0

    for row in rows:
        if isinstance(row, dict):
            for val in row.values():
                if val is None or val == "" or val == "null":
                    null_fields += 1

    completeness = round(((total_fields - null_fields) / total_fields) * 100) if total_fields > 0 else 0

    return {
        "completeness": completeness,
        "freshness": 90,
        "fieldCount": field_count
    }


def build_enriched_dataset(api_record, sample_data):
    """Build enriched dataset object from API record and sample."""
    name = (
        api_record.get("name") or
        api_record.get("Name") or
        api_record.get("title") or
        api_record.get("Title") or
        api_record.get("dataset_name") or
        api_record.get("DatasetName") or
        "Unknown Dataset"
    ).strip()

    return {
        "name": name,
        "status": normalize_status(
            api_record.get("status") or
            api_record.get("Status") or
            api_record.get("active") or ""
        ),
        "resolution": normalize_resolution(
            api_record.get("resolution") or
            api_record.get("Resolution") or
            api_record.get("res") or
            api_record.get("time_resolution") or ""
        ),
        "geo": normalize_geo(
            api_record.get("geo") or
            api_record.get("Geo") or
            api_record.get("geography") or
            api_record.get("Geography") or ""
        ),
        "org": get_organization(name),
        "recordCount": sample_data.get("total"),
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "qualityMetrics": compute_quality_metrics(sample_data.get("records", [])),
        "sampleData": sample_data.get("records", []),
        "description": api_record.get("description") or "No description available"
    }


def main():
    logger.info("=" * 60)
    logger.info("Energi Data Service — Enhanced Pipeline")
    logger.info("=" * 60)

    # 1. Fetch dataset metadata
    raw_records = fetch_dataset_metadata()
    if not raw_records:
        logger.warn("No datasets fetched. Skipping update.")
        return 1

    # 2. Enrich with sample data
    logger.info(f"Enriching {min(10, len(raw_records))} datasets with sample data...")
    enriched_datasets = []
    seen_names = set()

    for i, record in enumerate(raw_records[:10]):  # Sample first 10
        name = (
            record.get("name") or record.get("Name") or
            record.get("title") or record.get("Title") or "Unknown"
        ).strip()

        if name in seen_names:
            continue
        seen_names.add(name)

        sample = fetch_dataset_sample(name, limit=5)
        enriched = build_enriched_dataset(record, sample)
        enriched_datasets.append(enriched)
        logger.info(f"  [{i + 1}/10] Enriched: {name} ({enriched['recordCount']} records)")

    # Add remaining datasets without enrichment
    for record in raw_records[10:]:
        name = (
            record.get("name") or record.get("Name") or
            record.get("title") or record.get("Title") or "Unknown"
        ).strip()

        if name not in seen_names:
            seen_names.add(name)
            enriched = build_enriched_dataset(record, {"total": None, "records": []})
            enriched_datasets.append(enriched)

    logger.success(f"Enriched {len(enriched_datasets)} unique datasets")

    # 3. Build output data structure
    output = {
        "total": len(enriched_datasets),
        "organizations": {},
        "datasets": enriched_datasets,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # Count by organization
    for d in enriched_datasets:
        org = d["org"]
        output["organizations"][org] = output["organizations"].get(org, 0) + 1

    # 4. Save enriched JSON
    logger.info(f"Writing enriched data to {ENRICHED_JSON_FILE}...")
    try:
        with open(ENRICHED_JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, separators=(",", ":"))
        logger.success(f"Wrote {ENRICHED_JSON_FILE}")
    except IOError as e:
        logger.error(f"Failed to write {ENRICHED_JSON_FILE}: {e}")
        return 1

    # 5. Update HTML dashboard
    logger.info(f"Updating {DASHBOARD_FILE}...")
    try:
        with open(DASHBOARD_FILE, "r", encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        logger.error(f"{DASHBOARD_FILE} not found")
        return 1

    # Replace datasetsData const
    new_json = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
    pattern = r"(const datasetsData = )\{.*?\};"
    replacement = f"const datasetsData = {new_json};"
    new_html, count = re.subn(pattern, replacement, html, count=1, flags=re.DOTALL)

    if count == 0:
        logger.error("Could not find 'const datasetsData' in HTML file")
        return 1

    try:
        with open(DASHBOARD_FILE, "w", encoding="utf-8") as f:
            f.write(new_html)
        logger.success(f"Updated {DASHBOARD_FILE} with live data")
    except IOError as e:
        logger.error(f"Failed to write {DASHBOARD_FILE}: {e}")
        return 1

    # 6. Print summary
    logger.info("=" * 60)
    logger.success("Pipeline Complete")
    logger.info("=" * 60)
    logger.info(f"Total datasets: {len(enriched_datasets)}")
    for org, count in sorted(output["organizations"].items()):
        logger.info(f"  {org}: {count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
