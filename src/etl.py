import os
import sys
import time
import logging
import re
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

def env_or_default(key: str, default: str) -> str:
    """Return env var if set and non-empty; otherwise return default."""
    v = os.getenv(key)
    return v.strip() if v and v.strip() else default

# ---------------------- Config ----------------------
SAP_BASE_URL   = env_or_default("SAP_BASE_URL", "https://my438923.businessbydesign.cloud.sap").rstrip("/")
SAP_ODATA_PATH = env_or_default("SAP_ODATA_PATH", "/sap/byd/odata/ana_businessanalytics_analytics.svc").strip("/")
SAP_CODES_QUERY = env_or_default("SAP_CODES_QUERY", "RPZDA829C3EFFC649C58434CCQueryResults").strip("/")
SAP_MAIN_QUERY  = env_or_default("SAP_MAIN_QUERY",  "RPZDA829C3EFFC649C58434CCQueryResults").strip("/")
OUTPUT_CSV      = env_or_default("OUTPUT_CSV", "data/employee_data.csv")

SAP_USERNAME   = os.getenv("SAP_USERNAME")
SAP_PASSWORD   = os.getenv("SAP_PASSWORD")

REQUEST_PAUSE = float(env_or_default("REQUEST_PAUSE", "0.2"))  # seconds between calls

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "Content-Type": "application/json",
})

# Your intended fields (if SAP rejects one, we auto-drop it and retry)
SELECT_FIELDS = [
    "FCABSENCE_TIME","UCABSENCE_TIME","C0DATEFROM","C0DATETO","CEMPLOYEE_UUID","TEMPLOYEE_UUID",
    "RCHEADCOUNT","FCHEADCOUNT","UCHEADCOUNT","FCPLANNED_TIME","UCPLANNED_TIME","CPROJECT_UUID","TPROJECT_UUID",
    "FCRECORDED_TIME","UCRECORDED_TIME","RCCOMPLIANCE_RATE","FCCOMPLIANCE_RATE","UCCOMPLIANCE_RATE",
    "KCABSENCE_TIME","KCHEADCOUNT","KCPLANNED_TIME","KCRECORDED_TIME","KCCOMPLIANCE_RATE","C0CHAR_STRUCTURE"
]

RENAME_MAP = {
    "TEMPLOYEE_UUID": "Employee",
    "UCRECORDED_TIME": "Recorded Time",
    "C0DATEFROM": "Date From",
    "C0DATETO": "Date To",
    "TPROJECT_UUID": "Project ID",
    "KCPLANNED_TIME": "Planned Time",
    "KCRECORDED_TIME": "KC_Recorded Time",
}

# ---------------------- URL / OData helpers ----------------------
def _auth() -> Optional[HTTPBasicAuth]:
    if SAP_USERNAME and SAP_PASSWORD:
        return HTTPBasicAuth(SAP_USERNAME, SAP_PASSWORD)
    return None

def _root_url() -> str:
    return f"{SAP_BASE_URL.rstrip('/')}/{SAP_ODATA_PATH.strip('/')}".rstrip("/")

def _entity_url(entity: str) -> str:
    return f"{_root_url()}/{entity.strip('/')}".rstrip("/")

def _get_raw(url: str, params: Dict[str, str]) -> requests.Response:
    return SESSION.get(url, params=params, auth=_auth(), timeout=60)

def _get_json_or_raise(url: str, params: Dict[str, str]) -> Dict:
    resp = _get_raw(url, params)
    if not resp.ok:
        logging.error(
            "HTTP %s for %s params=%s\nBody: %s",
            resp.status_code, url, params, resp.text[:2000]
        )
        resp.raise_for_status()
    return resp.json()

def _extract_results_and_next(data: Dict) -> Tuple[List[Dict], Optional[str]]:
    # OData v2
    if "d" in data:
        d = data["d"]
        return d.get("results", []), d.get("__next")
    # newer
    return data.get("value", []), data.get("@odata.nextLink") or data.get("odata.nextLink")

def _extract_missing_segment(resp_text: str) -> Optional[str]:
    # Typical message: "Resource not found for the segment 'FCABSENCE_TIME'."
    m = re.search(r"segment\s+'([^']+)'", resp_text)
    return m.group(1) if m else None

# ---------------------- Core ETL ----------------------
def fetch_distinct_access_codes(top: int = 10000) -> List[str]:
    url = _entity_url(SAP_CODES_QUERY)
    params = {"$select": "C0CHAR_STRUCTURE", "$top": str(top)}
    data = _get_json_or_raise(url, params)
    results, _ = _extract_results_and_next(data)
    codes = [r.get("C0CHAR_STRUCTURE") for r in results if r.get("C0CHAR_STRUCTURE")]
    distinct = sorted(set(codes))
    logging.info("Fetched %d distinct access codes", len(distinct))
    return distinct

def _paged_fetch(url: str, params: Dict[str, str]) -> List[Dict]:
    all_rows: List[Dict] = []
    next_url = url
    next_params = dict(params)

    while True:
        data = _get_json_or_raise(next_url, next_params)
        rows, next_link = _extract_results_and_next(data)
        all_rows.extend(rows)
        if not next_link:
            break
        next_url, next_params = next_link, {}
        time.sleep(REQUEST_PAUSE)

    return all_rows

def fetch_rows_for_access_code(code: str, top_per_page: int = 5000) -> List[Dict]:
    """
    Fetch all rows for a code. If SAP rejects a $select field with
    "Resource not found for the segment 'FIELD'", auto-drop FIELD and retry.
    """
    base_url = _entity_url(SAP_MAIN_QUERY)

    filter_value = code.replace("'", "''")
    working_fields = list(SELECT_FIELDS)

    while True:
        select = ",".join(working_fields)
        params = {
            "$select": select,
            "$top": str(top_per_page),
            "$filter": f"C0CHAR_STRUCTURE eq '{filter_value}'",
        }

        resp = _get_raw(base_url, params)

        if resp.ok:
            data = resp.json()
            rows, next_link = _extract_results_and_next(data)

            all_rows = list(rows)
            # follow paging if present
            while next_link:
                time.sleep(REQUEST_PAUSE)
                data2 = _get_json_or_raise(next_link, {})
                rows2, next_link = _extract_results_and_next(data2)
                all_rows.extend(rows2)

            logging.info("  %s -> %d rows (fields=%d)", code, len(all_rows), len(working_fields))
            return all_rows

        # If 404 and message indicates a missing segment/field, drop it and retry
        if resp.status_code == 404:
            missing = _extract_missing_segment(resp.text)
            if missing and missing in working_fields and len(working_fields) > 1:
                working_fields.remove(missing)
                logging.warning("SAP rejected field '%s' — dropping it and retrying...", missing)
                continue

        # otherwise, raise
        logging.error("HTTP %s for %s params=%s\nBody: %s", resp.status_code, base_url, params, resp.text[:2000])
        resp.raise_for_status()

def _stringify_unhashables(x: Any) -> Any:
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

    # Put desired columns first if they exist
    ordered = [RENAME_MAP.get(c, c) for c in SELECT_FIELDS]
    first = [c for c in ordered if c in df.columns]
    rest = [c for c in df.columns if c not in first]
    df = df[first + rest]

    df = df.map(_stringify_unhashables).drop_duplicates()
    return df

def main():
    logging.info("Starting SAP OData ETL...")
    logging.info("Using root URL: %s", _root_url())
    logging.info("Codes entity: %s", SAP_CODES_QUERY)
    logging.info("Main entity: %s", SAP_MAIN_QUERY)
    logging.info("Output CSV: %s", OUTPUT_CSV)

    df = run_etl()

    out_path = OUTPUT_CSV
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    logging.info("Wrote %d rows to %s", len(df), out_path)

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
