#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
public_price_stock — SharePoint Table -> XML
- Reads ALL columns from SharePoint Excel table (env: SP_TABLE_NAME).
- Replaces column "Stock" (any case) with STRING buckets: 0, <10, <50, >50.
- Adds per-row <updatedAt> with format dd.mm.yyyy hh:mm (UTC+05:00).
- Writes UTF-8 XML to docs/public_price_stock.xml:
    <items><item><ColA>...</ColA>...<updatedAt>...</updatedAt></item>...</items>

Env (set as repo secrets):
  TENANT_ID, CLIENT_ID, CLIENT_SECRET
  SP_SITE_HOSTNAME, SP_SITE_PATH, SP_XLSX_PATH, SP_TABLE_NAME
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

# ---------- ENV ----------
def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v.strip()

TENANT_ID        = env("TENANT_ID")
CLIENT_ID        = env("CLIENT_ID")
CLIENT_SECRET    = env("CLIENT_SECRET")
SP_SITE_HOSTNAME = env("SP_SITE_HOSTNAME")   # e.g. bavatools.sharepoint.com
SP_SITE_PATH     = env("SP_SITE_PATH")       # e.g. /sites/Einhell_common
SP_XLSX_PATH     = env("SP_XLSX_PATH")       # e.g. /Shared Documents/.../Bava_data.xlsx
SP_TABLE_NAME    = env("SP_TABLE_NAME")      # _public_price_table

GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]
SAFE_PATH   = "/:+()%!$&',;=@"

# +05:00 (your required local time)
UTC_PLUS_5 = timezone(timedelta(hours=5))

# ---------- GRAPH HELPERS ----------
def get_token() -> str:
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"MS Graph auth failed: {result}")
    return result["access_token"]

def gget(url: str, token: str, timeout: int = 45) -> dict:
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"Graph GET {r.status_code}: {r.text[:400]}")
    return r.json()

def gget_raw(url: str, token: str, timeout: int = 45) -> requests.Response:
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
    # 1) direct path with common alternates
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

    # 2) search by name; match parent path suffix
    filename = os.path.basename(unquote(SP_XLSX_PATH))
    parent_dir = os.path.dirname(unquote(SP_XLSX_PATH)).replace("\\", "/")
    variants = {
        parent_dir,
        parent_dir.replace("/Shared Documents", "/Documents"),
        parent_dir.replace("/Documents", "/Shared Documents"),
        parent_dir.replace("/Shared Documents/", "/") if parent_dir.startswith("/Shared Documents/") else parent_dir,
        parent_dir.replace("/Documents/", "/") if parent_dir.startswith("/Documents/") else parent_dir,
    }
    for it in search_item(site_id, filename, token):
        parent = it.get("parentReference", {}).get("path", "")
        if any(parent.endswith(v) or ("/drive/root:" + v) in parent for v in variants):
            logging.info(f"Resolved workbook by search: {it.get('name')} @ {parent}")
            return it["id"]

    raise RuntimeError("Excel workbook not found via Graph.")

# ---------- DATA ----------
def read_table(site_id: str, item_id: str, token: str) -> pd.DataFrame:
    base = f"{GRAPH_BASE}/sites/{site_id}/drive/items/{item_id}/workbook/tables/{quote(SP_TABLE_NAME, safe='')}"
    hdr_values = gget(f"{base}/headerRowRange", token).get("values", [[]])
    headers = [str(h).strip() for h in (hdr_values[0] if hdr_values else [])]
    body = gget(f"{base}/dataBodyRange", token).get("values", []) or []
    df = pd.DataFrame(body, columns=headers)

    # normalize everything to string
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df

def to_bucket(stock_raw: str) -> str:
    """ Map any stock text to: 0, <10, <50, >50 (strings). """
    try:
        val = stock_raw.replace(",", ".").strip()
        n = int(float(val)) if val not in ("", "None", "nan") else 0
    except Exception:
        n = 0
    if n <= 0:  return "0"
    if n < 10:  return "<10"
    if n < 50:  return "<50"
    return ">50"

def sanitize_tag(name: str) -> str:
    """XML-safe tag names while keeping PQ-friendly names."""
    import re
    s = name.strip()
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    if not s or not s[0].isalpha():
        s = f"col_{s}"
    return s

def build_xml(df: pd.DataFrame, ts_str: str) -> bytes:
    # Replace "Stock" with bucketed strings (case-insensitive)
    lower_map = {c.lower(): c for c in df.columns}
    if "stock" in lower_map:
        stock_col = lower_map["stock"]
        df[stock_col] = df[stock_col].apply(to_bucket)
    else:
        logging.warning("Column 'Stock' not found; skipping bucketing")

    # Add/overwrite per-row timestamp
    df["updatedAt"] = ts_str

    root = etree.Element("items")
    for _, row in df.iterrows():
        item = etree.SubElement(root, "item")
        for col in df.columns:
            tag = sanitize_tag(col)
            val = "" if pd.isna(row[col]) else str(row[col])
            etree.SubElement(item, tag).text = val

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", pretty_print=True)

def main() -> int:
    try:
        token   = get_token()
        site_id = resolve_site_id(token)
        item_id = resolve_item_id(site_id, token)
        df      = read_table(site_id, item_id, token)

        ts = datetime.now(UTC_PLUS_5).strftime("%d.%m.%Y %H:%M")
        xml_bytes = build_xml(df, ts)

        out_path = "docs/public_price_stock.xml"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "wb") as f:
            f.write(xml_bytes)

        logging.info(f"SUCCESS: wrote {out_path} with {len(df)} rows")
        return 0
    except Exception:
        logging.exception("Run failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
