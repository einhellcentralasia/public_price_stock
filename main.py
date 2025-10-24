#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SharePoint Table -> XML exporter (public_price_stock)
- Reads table: _public_price_table from the workbook path provided in env.
- Uses ALL present columns dynamically (no schema hardcoding).
- Rewrites "Stock" numeric into STRING buckets: 0, <10, <50, >50.
- Adds per-row <updatedAt> in dd.mm.yyyy hh:mm (UTC+05:00 per your requirement).
- Writes docs/public_price_stock.xml

Env (GitHub Secrets):
  TENANT_ID, CLIENT_ID, CLIENT_SECRET
  SP_SITE_HOSTNAME, SP_SITE_PATH, SP_XLSX_PATH, SP_TABLE_NAME  (=_public_price_table)
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, unquote

import pandas as pd
import requests
from lxml import etree
import msal

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ---------- helpers ----------
def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v.strip()

TENANT_ID        = env("TENANT_ID")
CLIENT_ID        = env("CLIENT_ID")
CLIENT_SECRET    = env("CLIENT_SECRET")

SP_SITE_HOSTNAME = env("SP_SITE_HOSTNAME")  # e.g., bavatools.sharepoint.com
SP_SITE_PATH     = env("SP_SITE_PATH")      # e.g., /sites/Einhell_common
SP_XLSX_PATH     = env("SP_XLSX_PATH")      # e.g., /Shared Documents/General/_system_files/Bava_data.xlsx
SP_TABLE_NAME    = env("SP_TABLE_NAME")     # _public_price_table

GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]
SAFE_PATH   = "/:+()%!$&',;=@"

# Timezone +05:00 as requested (Kazakhstan Almaty)
UTC_PLUS_5 = timezone(timedelta(hours=5))

def get_token() -> str:
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"MS Graph auth failed: {result}")
    return result["access_token"]

def gget(url: str, token: str, timeout: int = 40) -> dict:
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"Graph GET {url} failed {r.status_code}: {r.text[:400]}")
    return r.json()

def gget_raw(url: str, token: str, timeout: int = 40) -> requests.Response:
    return requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)

def resolve_site_id(token: str) -> str:
    data = gget(f"{GRAPH_BASE}/sites/{SP_SITE_HOSTNAME}:{SP_SITE_PATH}", token)
    return data["id"]

def try_item_by_path(site_id: str, path: str, token: str):
    path = path if path.startswith("/") else "/" + path
    quoted = quote(path, safe=SAFE_PATH)
    url = f"{GRAPH_BASE}/sites/{site_id}/drive/root:{quoted}"
    return gget_raw(url, token)

def search_item(site_id: str, filename: str, token: str):
    q = quote(filename, safe="")
    url = f"{GRAPH_BASE}/sites/{site_id}/drive/root/search(q='{q}')"
    return gget(url, token).get("value", [])

def resolve_item_id(site_id: str, token: str) -> str:
    # Try direct path and common alternates
    candidates = {
        SP_XLSX_PATH,
        SP_XLSX_PATH.replace("/Shared Documents", "/Documents"),
        SP_XLSX_PATH.replace("/Documents", "/Shared Documents"),
        SP_XLSX_PATH.replace("/Shared Documents/", "/") if SP_XLSX_PATH.startswith("/Shared Documents/") else SP_XLSX_PATH,
        SP_XLSX_PATH.replace("/Documents/", "/") if SP_XLSX_PATH.startswith("/Documents/") else SP_XLSX_PATH,
    }
    for p in candidates:
        r = try_item_by_path(site_id, p, token)
        if r.status_code < 400:
            logging.info(f"Resolved workbook by path: {p}")
            return r.json()["id"]

    # Fallback: search by name, then match parent path suffix
    filename = os.path.basename(unquote(SP_XLSX_PATH))
    folder   = os.path.dirname(unquote(SP_XLSX_PATH)).replace("\\", "/")
    variants = {
        folder,
        folder.replace("/Shared Documents", "/Documents"),
        folder.replace("/Documents", "/Shared Documents"),
        folder.replace("/Shared Documents/", "/") if folder.startswith("/Shared Documents/") else folder,
        folder.replace("/Documents/", "/") if folder.startswith("/Documents/") else folder,
    }
    for it in search_item(site_id, filename, token):
        parent = it.get("parentReference", {}).get("path", "")
        if any(parent.endswith(v) or ("/drive/root:" + v) in parent for v in variants):
            logging.info(f"Resolved workbook by search: {it.get('name')} @ {parent}")
            return it["id"]

    raise RuntimeError("Excel workbook not found via Microsoft Graph.")

def read_table(site_id: str, item_id: str, token: str) -> pd.DataFrame:
    base = f"{GRAPH_BASE}/sites/{site_id}/drive/items/{item_id}/workbook/tables/{quote(SP_TABLE_NAME, safe='')}"
    headers = gget(f"{base}/headerRowRange", token).get("values", [[]])
    headers = [str(h).strip() for h in (headers[0] if headers else [])]

    body = gget(f"{base}/dataBodyRange", token).get("values", []) or []
    df = pd.DataFrame(body, columns=headers)

    # Normalize: ensure all columns are strings by default
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    return df

def to_bucket(stock_raw: str) -> str:
    """Map any stock text to buckets: 0, <10, <50, >50."""
    try:
        n = int(float(stock_raw.replace(",", ".").strip())) if stock_raw not in ("", "None", "nan") else 0
    except Exception:
        n = 0
    if n <= 0:
        return "0"
    if n < 10:
        return "<10"
    if n < 50:
        return "<50"
    return ">50"

def sanitize_tag(name: str) -> str:
    """Make XML-safe tag names while keeping them readable for Power Query."""
    import re
    s = name.strip()
    # Replace spaces and forbidden chars with underscore
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    # Tag can't start with digit or punctuation like '-' or '.'
    if not s or not s[0].isalpha():
        s = f"col_{s}"
    return s

def df_to_xml(df: pd.DataFrame, out_path: str, ts_str: str):
    # Prepare: replace "Stock" with bucketed strings, add updatedAt column
    col_map = {c.lower(): c for c in df.columns}
    if "stock" in col_map:
        src = col_map["stock"]
        df[src] = df[src].apply(to_bucket)
    else:
        logging.warning("Column 'Stock' not found; no bucketing applied.")

    # Add (or overwrite) updatedAt column
    df["updatedAt"] = ts_str

    # Build XML
    root = etree.Element("items")
    for _, row in df.iterrows():
        item = etree.SubElement(root, "item")
        for col in df.columns:
            val = "" if pd.isna(row[col]) else str(row[col])
            tag = sanitize_tag(col)
            etree.SubElement(item, tag).text = val

    xml_bytes = etree.tostring(root, xml_declaration=True, encoding="UTF-8", pretty_print=True)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(xml_bytes)

def main() -> int:
    try:
        token = get_token()
        site_id = resolve_site_id(token)
        item_id = resolve_item_id(site_id, token)
        df = read_table(site_id, item_id, token)

        # Timestamp in dd.mm.yyyy hh:mm with +05:00 (as string, no timezone suffix)
        ts = datetime.now(UTC_PLUS_5).strftime("%d.%m.%Y %H:%M")

        out_file = "docs/public_price_stock.xml"
        df_to_xml(df, out_file, ts)
        logging.info(f"SUCCESS: wrote {out_file} with {len(df)} rows.")
        print("OK")
        return 0
    except Exception as e:
        logging.exception("Run failed")
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
