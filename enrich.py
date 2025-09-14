#!/usr/bin/env python3
"""
enrich_and_push.py

Full pipeline:
 - Query SQLite DB (client_data.db) for customers in 'Manchester' with orders since 2024-09-01.
 - Enrich each customer via GET http://localhost:5000/enrichment?email=...
   (requires header X-API-KEY: SECRET_KEY_123)
 - Push enriched row back to API (POST http://localhost:5000/submit by default).
 - Produce CSV: customer_id,name,email,total_spend,social_handle,success,reason

Notes:
 - If the push endpoint differs, set env var PUSH_PATH or edit PUSH_PATH below.
 - This script implements robust retry/backoff including handling 429 Retry-After,
   random server errors (5xx), missing profiles (404), and auth (401).
"""

import argparse
import csv
import json
import logging
import os
import random
import sqlite3
import sys
import time
from typing import Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# ------- Configuration -------
API_BASE = os.environ.get("API_BASE", "http://localhost:5000")
API_KEY = os.environ.get("API_KEY", "SECRET_KEY_123")
PUSH_PATH = os.environ.get("PUSH_PATH", "/submission")
DB_PATH = os.environ.get("DB_PATH", os.path.join("database", "client_data.db"))
OUTPUT_CSV = os.environ.get("OUTPUT_CSV", "enriched_customers.csv")

# HTTP retry config
RETRY_TOTAL = 5
BACKOFF_FACTOR = 1  # used by urllib3 Retry (exponential backoff: {backoff_factor} * (2 ** (retry - 1)))
STATUS_FORCELIST = [429, 500, 502, 503, 504]

# SQL query - uses schema from DATABASE.md (customers.city, orders.order_date, orders.order_total)
SQL_QUERY = """
SELECT c.customer_id, c.name, c.email, SUM(o.order_total) AS total_spend
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE LOWER(c.city) = 'manchester'
  AND o.order_date >= '2024-09-01 00:00:00'
GROUP BY c.customer_id, c.name, c.email
ORDER BY total_spend DESC;
"""

# CSV header (exact order required by task)
CSV_FIELDNAMES = ["customer_id", "name", "email", "total_spend", "social_handle", "success", "reason"]

# ------- Logging -------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("enrich_and_push.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("enrich_and_push")


# ------- HTTP helpers -------
def make_session() -> requests.Session:
    """
    Create a requests.Session with urllib3 Retry configured.
    It will retry on STATUS_FORCELIST and respect Retry-After header for 429.
    """
    session = requests.Session()
    retries = Retry(
        total=RETRY_TOTAL,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=STATUS_FORCELIST,
        allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"X-API-KEY": API_KEY, "Accept": "application/json"})
    return session


def jitter_sleep(base_seconds: float):
    """Sleep base_seconds plus small random jitter to reduce thundering-herd."""
    sleep_time = base_seconds + random.random() * 0.5
    time.sleep(sleep_time)


# ------- Database -------
def load_customers_from_db(db_path: str = DB_PATH) -> list:
    """
    Run the SQL query and return a list of dict rows:
    {customer_id, name, email, total_spend}
    """
    if not os.path.exists(db_path):
        logger.error("Database file not found at: %s", db_path)
        raise FileNotFoundError(db_path)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(SQL_QUERY)
        rows = cur.fetchall()
        results = []
        for r in rows:
            customer_id, name, email, total_spend = r
            # defensive normalization
            email = (email or "").strip().lower()
            name = (name or "").strip()
            try:
                total_spend = float(total_spend) if total_spend is not None else 0.0
            except Exception:
                total_spend = 0.0
            results.append({
                "customer_id": customer_id,
                "name": name,
                "email": email,
                "total_spend": total_spend,
            })
        logger.info("Loaded %d customers from DB", len(results))
        return results
    finally:
        conn.close()


# ------- Enrichment & Push logic -------
def get_social_handle(session: requests.Session, email: str, max_local_attempts: int = 5) -> Tuple[Optional[str], str]:
    """
    Call GET {API_BASE}/enrichment?email={email}
    Return (social_handle_or_none, reason_string)
    Handles 401, 404 (missing profile), 429 Retry-After and 5xx with retries.
    """
    url = f"{API_BASE.rstrip('/')}/enrichment"
    params = {"email": email}
    for attempt in range(1, max_local_attempts + 1):
        try:
            resp = session.get(url, params=params, timeout=10)
        except requests.RequestException as e:
            logger.warning("Network error when enriching %s (attempt %d/%d): %s", email, attempt, max_local_attempts, e)
            jitter_sleep(2 ** attempt)
            continue

        # Auth
        if resp.status_code == 401:
            logger.error("Unauthorized (401) when enriching %s. Check API_KEY.", email)
            return None, "unauthorized"

        # Missing profile
        if resp.status_code == 404:
            logger.info("No profile found for %s (404).", email)
            return None, "no_profile"

        # Successful response
        if resp.ok:
            try:
                data = resp.json()
            except json.JSONDecodeError:
                logger.warning("Invalid JSON for enrichment response for %s: %s", email, resp.text[:200])
                return None, "invalid_json"

            # we expect a field 'social_handle' per task
            social = None
            if isinstance(data, dict):
                social = data.get("social_handle") or data.get("handle") or data.get("social")
            if social:
                return social, "ok"
            # if response is empty or missing handle, treat as missing
            return None, "no_social_handle"

        # Rate limited - check Retry-After then backoff
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            try:
                wait = int(ra) if ra is not None else (2 ** attempt)
            except Exception:
                wait = 2 ** attempt
            logger.warning("429 for enrichment %s; waiting %s seconds (Retry-After=%s).", email, wait, ra)
            time.sleep(wait + random.random())
            continue

        # 5xx or unexpected - let the urllib3 Retry handle retries, but also add local backoff
        if 500 <= resp.status_code < 600:
            logger.warning("Server error %d for enrichment %s (attempt %d).", resp.status_code, email, attempt)
            jitter_sleep(2 ** attempt)
            continue

        # Other client errors - treat as failure
        logger.error("Unexpected response %d for enrichment %s: %s", resp.status_code, email, resp.text[:200])
        return None, f"enrich_error_{resp.status_code}"

    return None, "enrich_max_attempts"


def push_enriched_row(session: requests.Session, payload: Dict, max_local_attempts: int = 5) -> Tuple[bool, str]:
    """
    POST the enriched payload to API_BASE + PUSH_PATH.
    Returns (success_bool, reason_string).
    Handles 401, 429, 5xx, etc.
    """
    url = f"{API_BASE.rstrip('/')}{PUSH_PATH}"
    headers = {"Content-Type": "application/json"}

    for attempt in range(1, max_local_attempts + 1):
        try:
            resp = session.post(url, json=payload, headers=headers, timeout=10)
        except requests.RequestException as e:
            logger.warning("Network error when pushing %s (attempt %d/%d): %s", payload.get("email"), attempt, max_local_attempts, e)
            jitter_sleep(2 ** attempt)
            continue

        if resp.status_code == 401:
            logger.error("Unauthorized (401) when pushing %s. Check API_KEY.", payload.get("email"))
            return False, "unauthorized"

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            try:
                wait = int(ra) if ra is not None else (2 ** attempt)
            except Exception:
                wait = 2 ** attempt
            logger.warning("429 pushing %s; waiting %s seconds (Retry-After=%s).", payload.get("email"), wait, ra)
            time.sleep(wait + random.random())
            continue

        if resp.ok:
            # Consider successful push
            return True, "pushed_ok"

        if 400 <= resp.status_code < 500:
            # client-side error: don't retry
            logger.error("Client error %d when pushing %s: %s", resp.status_code, payload.get("email"), resp.text[:300])
            return False, f"client_error_{resp.status_code}"

        if 500 <= resp.status_code < 600:
            # server error - retry
            logger.warning("Server error %d when pushing %s (attempt %d).", resp.status_code, payload.get("email"), attempt)
            jitter_sleep(2 ** attempt)
            continue

        # unexpected
        logger.error("Unexpected response %d when pushing %s: %s", resp.status_code, payload.get("email"), resp.text[:300])
        return False, f"push_error_{resp.status_code}"

    return False, "push_max_attempts"


# ------- Main pipeline -------
def run_pipeline(dry_run: bool = False, db_path: str = DB_PATH, output_csv_path: str = OUTPUT_CSV):
    session = make_session()
    customers = load_customers_from_db(db_path=db_path)

    results = []
    logger.info("Starting enrichment & push for %d customers (dry_run=%s)", len(customers), dry_run)

    for cust in tqdm(customers, desc="Processing customers", unit="cust"):
        customer_id = cust["customer_id"]
        name = cust["name"]
        email = cust["email"]
        total_spend = cust["total_spend"]

        if not email:
            logger.warning("Skipping customer %s due to missing email", customer_id)
            results.append({
                "customer_id": customer_id,
                "name": name,
                "email": email,
                "total_spend": total_spend,
                "social_handle": "",
                "success": "false",
                "reason": "missing_email",
            })
            continue

        social_handle, enrich_reason = get_social_handle(session, email)
        # build base result row
        row = {
            "customer_id": customer_id,
            "name": name,
            "email": email,
            "total_spend": total_spend,
            "social_handle": social_handle or "",
            "success": "false",
            "reason": enrich_reason,
        }

        if social_handle:
            # prepare payload to push. Adjust fields if API expects different schema.
            payload = {
                "customer_id": customer_id,
                "name": name,
                "email": email,
                "total_spend": total_spend,
                "social_handle": social_handle,
            }
            if dry_run:
                logger.info("Dry-run: would push payload for %s: %s", email, payload)
                row["success"] = "true"
                row["reason"] = "dry_run_ok"
            else:
                pushed, push_reason = push_enriched_row(session, payload)
                row["success"] = "true" if pushed else "false"
                row["reason"] = push_reason
        else:
            # No social handle found; do not attempt push (task implies push for enriched rows)
            logger.info("No social handle for %s: %s", email, enrich_reason)
            # row already has success=false and reason=enrich_reason

        results.append(row)
        # small jitter between customers to reduce burstiness
        time.sleep(0.05 + random.random() * 0.05)

    # write CSV
    logger.info("Writing %d rows to CSV %s", len(results), output_csv_path)
    with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for r in results:
            writer.writerow({
                "customer_id": r["customer_id"],
                "name": r["name"],
                "email": r["email"],
                "total_spend": r["total_spend"],
                "social_handle": r["social_handle"],
                "success": r["success"],
                "reason": r["reason"],
            })

    # summary
    total = len(results)
    success_count = sum(1 for r in results if r["success"].lower() == "true")
    logger.info("Finished. Total=%d, Successful pushes=%d", total, success_count)
    print(f"Finished. Total={total}, Successful pushes={success_count}. CSV: {output_csv_path}")


# ------- CLI -------
def parse_args():
    p = argparse.ArgumentParser(description="Enrich customers from DB and push enriched rows to API.")
    p.add_argument("--db", default=DB_PATH, help="Path to SQLite DB (default: client_data.db)")
    p.add_argument("--out", default=OUTPUT_CSV, help="Output CSV file path")
    p.add_argument("--dry-run", action="store_true", help="Don't POST pushes; only perform enrichment and write CSV")
    p.add_argument("--api-base", default=API_BASE, help="API base URL (default: http://localhost:5000)")
    p.add_argument("--push-path", default=PUSH_PATH, help="Push endpoint path (default: /submit)")
    return p.parse_args()


def main():
    args = parse_args()
    global API_BASE, PUSH_PATH
    API_BASE = args.api_base
    PUSH_PATH = args.push_path

    # Show where we'll talk to and what DB we'll use
    logger.info("Using API_BASE=%s PUSH_PATH=%s DB=%s", API_BASE, PUSH_PATH, args.db)

    try:
        run_pipeline(dry_run=args.dry_run, db_path=args.db, output_csv_path=args.out)
    except Exception as e:
        logger.exception("Unhandled error in pipeline: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
