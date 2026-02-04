import os
import sys
import time
import logging
import re
from typing import Dict, List, Tuple, Optional, Any

import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

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

def env_or_default(key: str, default: str) -> str:
    v = os.getenv(key)
    return v.strip() if v and v.strip() else default

# ---------------------- Config ----------------------
SAP_BASE_URL    = env_or_default("SAP_BASE_URL", "https://my438923.businessbydesign.cloud.sap").rstrip("/")
SAP_ODATA_PATH  = env_or_default("SAP_ODATA_PATH", "/sap/byd/odata/ana_businessanalytics_analytics.svc").strip("/")
SAP_CODES_QUERY = env_or_default("SAP_CODES_QUERY", "RPZ13203A10283FF8DF90F8B6QueryResults").strip("/")
SAP_MAIN_QUERY  = env_or_default("SAP_MAIN_QUERY",  "RPZ13203A10283FF8DF90F8B6QueryResults").strip("/")
OUTPUT_CSV      = env_or_default("OUTPUT_CSV", "data/employee_data.csv")

SAP_USERNAME    = os.getenv("SAP_USERNAME")
SAP_PASSWORD    = os.getenv("SAP_PASSWORD")

REQUEST_PAUSE = float(env_or_default("REQUEST_PAUSE", "0.2"))

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})

# Landon’s “main” fields (keep your core deliverables)
BASE_SELECT_FIELDS = [
    "TEMPLOYEE_UUID",
    "CEMPLOYEE_UUID",
    "C0DATEFROM",
    "C0DATETO",
    "KCLEAVERS",
    # structure field will be appended after we detect which one exists
]

RENAME_MAP = {
    "TEMPLOYEE_UUID": "Employee",
    "CEMPLOYEE_UUID": "Employee ID",
    "C0DATEFROM": "Date From",
    "C0DATETO": "Date To",
    "KCLEAVERS": "K Cleavers",
    "COCHAR_STRUCTURE": "Structure",
    "C0CHAR_STRUCTURE": "Structure",
}

# Candidate structure field names (ByD can use either)
STRUCT_CANDIDATES = ["COCHAR_STRUCTURE", "C0CHAR_STRUCTURE"]

# ---------------------- URL helpers ----------------------
def _auth() -> Optional[HTTPBasicAuth]:
    if SAP_USERNAME and SAP_PASSWORD:
        return HTTPBasicAuth(SAP_USERNAME, SAP_PASSWORD)
    return None

def _root_url() -> str:
    return f"{SAP_BASE_URL.rstrip('/')}/{SAP_ODATA_PATH.strip('/')}".rstrip("/")

def _entity_url(entity: str) -> str:
    return f"{_root_url()}/{entity.strip('/')}".rstrip("/")

def _get_raw(url: str, params: Dict[str, str]) -> requests.Response:
    return SESSION.get(url, params=params, auth=_auth(), timeout=90)

def _get_json_or_raise(url: str, params: Dict[str, str]) -> Dict:
    resp = _get_raw(url, params)
    if not resp.ok:
        logging.error("HTTP %s for %s params=%s\nBody: %s",
                      resp.status_code, url, params, resp.text[:2000])
        resp.raise_for_status()
    return resp.json()

def _extract_results_and_next(data: Dict) -> Tuple[List[Dict], Optional[str]]:
    if "d" in data:
        d = data["d"]
        return d.get("results", []), d.get("__next")
    return data.get("value", []), data.get("@odata.nextLink") or data.get("odata.nextLink")

def _extract_missing_segment(resp_text: str) -> Optional[str]:
    m = re.search(r"segment\s+'([^']+)'", resp_text)
    return m.group(1) if m else None

# ---------------------- Detect correct structure field ----------------------
def detect_structure_field(entity: str) -> str:
    """
    Try candidates via $select; return the one the service accepts.
    We use a tiny $top=1 request so it’s fast.
    """
    url = _entity_url(entity)
    for field in STRUCT_CANDIDATES:
        params = {"$select": field, "$top": "1", "$format": "json"}
        resp = _get_raw(url, params)
        if resp.ok:
            logging.info("Detected structure field: %s", field)
            return field
        # if SAP says this field doesn't exist, try the next
        if resp.status_code == 404:
            missing = _extract_missing_segment(resp.text)
            if missing == field:
                continue
        # other errors should surface
        logging.error("HTTP %s while detecting field %s\nBody: %s", resp.status_code, field, resp.text[:2000])
        resp.raise_for_status()

    raise RuntimeError("Could not detect a valid structure field (tried COCHAR_STRUCTURE and C0CHAR_STRUCTURE).")

# ---------------------- Core ETL ----------------------
def fetch_distinct_structures() -> Tuple[str, List[str]]:
    """
    Fetch distinct structures. IMPORTANT: your error proves COCHAR_STRUCTURE is not valid here,
    so we detect and use whichever exists (usually C0CHAR_STRUCTURE).
    """
    struct_field = detect_structure_field(SAP_CODES_QUERY)
    url = _entity_url(SAP_CODES_QUERY)
    params = {"$select": struct_field, "$top": "1000000", "$format": "json"}
    data = _get_json_or_raise(url, params)
    results, _ = _extract_results_and_next(data)

    vals = [r.get(struct_field) for r in results if r.get(struct_field)]
    distinct = sorted(set(vals))
    logging.info("Fetched %d distinct %s values", len(distinct), struct_field)
    return struct_field, distinct

def fetch_rows_for_structure(struct_field: str, structure_value: str, top_per_page: int = 1000000) -> List[Dict]:
    """
    Pull rows filtered by the detected structure field.
    """
    base_url = _entity_url(SAP_MAIN_QUERY)

    # Use the same detected structure field for filtering the same entity
    filter_value = structure_value.replace("'", "''")

    # Make sure the select includes the structure field too
    working_fields = list(BASE_SELECT_FIELDS)
    if struct_field not in working_fields:
        working_fields.append(struct_field)

    select = ",".join(working_fields)
    params = {
        "$select": select,
        "$top": str(top_per_page),
        "$format": "json",
        "$filter": f"{struct_field} eq '{filter_value}'",
    }

    resp = _get_raw(base_url, params)
    if not resp.ok:
        logging.error("HTTP %s for %s params=%s\nBody: %s", resp.status_code, base_url, params, resp.text[:2000])
        resp.raise_for_status()

    data = resp.json()
    rows, next_link = _extract_results_and_next(data)
    all_rows = list(rows)

    while next_link:
        time.sleep(REQUEST_PAUSE)
        data2 = _get_json_or_raise(next_link, {})
        rows2, next_link = _extract_results_and_next(data2)
        all_rows.extend(rows2)

    logging.info("  %s=%s -> %d rows", struct_field, structure_value, len(all_rows))
    return all_rows

def _stringify_unhashables(x: Any) -> Any:
    if isinstance(x, (dict, list, set)):
        return str(x)
    return x

def run_etl() -> pd.DataFrame:
    struct_field, structures = fetch_distinct_structures()
    all_records: List[Dict] = []

    for i, s in enumerate(structures, start=1):
        logging.info("(%d/%d) Fetching structure: %s", i, len(structures), s)
        try:
            all_records.extend(fetch_rows_for_structure(struct_field, s))
        except Exception as e:
            logging.exception("Failed for %s=%s: %s", struct_field, s, e)
        time.sleep(REQUEST_PAUSE)

    if not all_records:
        logging.warning("No records fetched.")
        return pd.DataFrame()

    df = pd.DataFrame.from_records(all_records).rename(columns=RENAME_MAP)

    # Put expected columns first if present
    ordered = [RENAME_MAP.get(c, c) for c in (BASE_SELECT_FIELDS + ["COCHAR_STRUCTURE", "C0CHAR_STRUCTURE"])]
    first = [c for c in ordered if c in df.columns]
    rest = [c for c in df.columns if c not in first]
    df = df[first + rest]

    df = df.map(_stringify_unhashables).drop_duplicates()
    return df

def main():
    logging.info("Starting SAP OData ETL...")
    logging.info("Root URL: %s", _root_url())
    logging.info("Entity: %s", SAP_MAIN_QUERY)
    logging.info("Output CSV: %s", OUTPUT_CSV)

    df = run_etl()

    out_path = OUTPUT_CSV
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    logging.info("Wrote %d rows to %s", len(df), out_path)

if __name__ == "__main__":
    main()
