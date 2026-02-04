import os
import sys
import time
import logging
from typing import Dict, List, Tuple, Optional, Any

import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

# Try to load a local .env if present (useful for local testing)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

# ---------------------- Config ----------------------
# TEMPORARY: hardcode base URL to avoid secret formatting issue
SAP_BASE_URL = "https://my438923.businessbydesign.cloud.sap"

# If/when secrets are good, replace the line above with:
# SAP_BASE_URL   = os.getenv("SAP_BASE_URL", "https://my438923.businessbydesign.cloud.sap").rstrip("/")

SAP_ODATA_PATH = os.getenv("SAP_ODATA_PATH", "/sap/byd/odata/ana_businessanalytics_analytics.svc").strip("/")
SAP_QUERY      = os.getenv("SAP_QUERY", "RPZDA829C3EFFC649C58434CCQueryResults").strip("/")
OUTPUT_CSV     = os.getenv("OUTPUT_CSV", "data/employee_data.csv")

SAP_USERNAME   = os.getenv("SAP_USERNAME")
SAP_PASSWORD   = os.getenv("SAP_PASSWORD")

REQUEST_PAUSE = float(os.getenv("REQUEST_PAUSE", "0.2"))  # seconds between calls

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "Content-Type": "application/json",
})

# Columns requested (mirrors the Power BI M code)
SELECT_FIELDS = [
    "FCABSENCE_TIME","UCABSENCE_TIME","C0DATEFROM","C0DATETO","CEMPLOYEE_UUID","TEMPLOYEE_UUID",
    "RCHEADCOUNT","FCHEADCOUNT","UCHEADCOUNT","FCPLANNED_TIME","UCPLANNED_TIME","CPROJECT_UUID","TPROJECT_UUID",
    "FCRECORDED_TIME","UCRECORDED_TIME","RCCOMPLIANCE_RATE","FCCOMPLIANCE_RATE","UCCOMPLIANCE_RATE",
    "KCABSENCE_TIME","KCHEADCOUNT","KCPLANNED_TIME","KCRECORDED_TIME","KCCOMPLIANCE_RATE","C0CHAR_STRUCTURE"
]

# Rename map to match the M step
RENAME_MAP = {
    "TEMPLOYEE_UUID": "Employee",
    "UCRECORDED_TIME": "Recorded Time",
    "C0DATEFROM": "Date From",
    "C0DATETO": "Date To",
    "TPROJECT_UUID": "Project ID",
    "KCPLANNED_TIME": "Planned Time",
    "KCRECORDED_TIME": "KC_Recorded Time",
}

# ---------------------- Helpers ----------------------
def _auth() -> Optional[HTTPBasicAuth]:
    if SAP_USERNAME and SAP_PASSWORD:
        return HTTPBasicAuth(SAP_USERNAME, SAP_PASSWORD)
    return None

def _root_url() -> str:
    return f"{SAP_BASE_URL}/{SAP_ODATA_PATH}".rstrip("/")

def _entity_url(entity: str) -> str:
    return f"{_root_url()}/{entity}".rstrip("/")

def _get(url: str, params: Dict[str, str]) -> Dict:
    """GET wrapper with basic auth, error handling, and JSON parsing."""
    resp = SESSION.get(url, params=params, auth=_auth(), timeout=60)
    if not resp.ok:
        logging.error("HTTP %s for %s params=%s\nBody: %s",
                      resp.status_code, url, params, resp.text[:1200])
        resp.raise_for_status()
    return resp.json()

def _extract_results_and_next(data: Dict) -> Tuple[List[Dict], Optional[str]]:
    """
    OData v2 shape:
      {'d': {'results': [...], '__next': '<absolute-url?...$skiptoken=...>'}}
    Fallbacks for newer shapes use '@odata.nextLink'/'odata.nextLink'.
    """
    if "d" in data:
        d = data["d"]
        return d.get("results", []), d.get("__next")
    return data.get("value", []), data.get("@odata.nextLink") or data.get("odata.nextLink")

# ---------------------- Core ETL ----------------------
def fetch_distinct_access_codes(top: int = 10000) -> List[str]:
    url = _entity_url(SAP_QUERY)
    params = {"$select": "C0CHAR_STRUCTURE", "$top": str(top)}
    data = _get(url, params)
    results, _ = _extract_results_and_next(data)
    codes = [r.get("C0CHAR_STRUCTURE") for r in results if r.get("C0CHAR_STRUCTURE")]
    distinct = sorted(set(codes))
    logging.info("Fetched %d distinct access codes", len(distinct))
    return distinct

def fetch_rows_for_access_code(code: str, top_per_page: int = 5000) -> List[Dict]:
    """Fetch all rows for a code, following server-driven paging via __next/skiptoken."""
    base_url = _entity_url(SAP_QUERY)
    select = ",".join(SELECT_FIELDS)

    # Escape single quotes *before* using in the f-string
    filter_value = code.replace("'", "''")

    params = {
        "$select": select,
        "$top": str(top_per_page),
        "$filter": f"C0CHAR_STRUCTURE eq '{filter_value}'",
    }

    all_rows: List[Dict] = []
    url = base_url
    while True:
        data = _get(url, params)
        rows, next_link = _extract_results_and_next(data)
        all_rows.extend(rows)
        if not next_link:
            break
        # __next is a full URL with skiptoken; follow it as-is
        url, params = next_link, {}
        time.sleep(REQUEST_PAUSE)

    logging.info("  %s -> %d rows", code, len(all_rows))
    return all_rows

def _stringify_unhashables(x: Any) -> Any:
    """Convert dict/list (and other unhashables) to string for deduping."""
    if isinstance(x, (dict, list, set)):
        return str(x)
    return x

def run_etl() -> pd.DataFrame:
    codes = fetch_distinct_access_codes()
    all_records: List[Dict] = []
    for i, code in enumerate(codes, start=1):
        logging.info("(%d/%d) Fetching code: %s", i, len(codes), code)
        try:
            rows = fetch_rows_for_access_code(code)
            all_records.extend(rows)
        except Exception as e:
            logging.exception("Failed for code %s: %s", code, e)
        time.sleep(REQUEST_PAUSE)

    if not all_records:
        logging.warning("No records fetched.")
        return pd.DataFrame()

    df = pd.DataFrame.from_records(all_records).rename(columns=RENAME_MAP)

    # Keep selected columns first, then any extras
    ordered = [RENAME_MAP.get(c, c) for c in SELECT_FIELDS]
    first = [c for c in ordered if c in df.columns]
    rest = [c for c in df.columns if c not in first]
    df = df[first + rest]

    # Normalize unhashable values before deduping
    df = df.map(_stringify_unhashables)

    # Now it's safe to drop duplicates
    df = df.drop_duplicates()

    return df

def main():
    if not (SAP_USERNAME and SAP_PASSWORD):
        logging.warning("SAP_USERNAME/SAP_PASSWORD not set; calls may fail if auth is required.")
    logging.info("Starting SAP OData ETL...")
    df = run_etl()
    out_path = OUTPUT_CSV
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    logging.info("Wrote %d rows to %s", len(df), out_path)

if __name__ == "__main__":
    main()
