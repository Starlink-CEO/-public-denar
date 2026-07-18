#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================
 PUBLIC DENAR — daily ingestion pipeline (production)
============================================================
Turns North Macedonia's open procurement data into the
feed.json consumed by the Public Denar app.

DATA FLOW (runs once a day via cron):

  1. EXTRACT   ESPP / e-nabavki Open Data module (CSV export)
               + data.gov.mk procurement datasets (CSV/JSON)
  2. ENRICH    Central Registry cross-reference:
               company age, founders, beneficial owners
  3. DETECT    red-flag engine (mirrors the indicator families
               of the official ESPP Red Flags system)
  4. EXPLAIN   Claude writes neutral trilingual summaries
               (MK / SQ / EN) — patterns, never accusations
  5. PUBLISH   feed.json  →  static hosting / CDN
               The app polls this file. That's the whole API.

DEPLOYMENT NOTES
  * ESPP open data: e-nabavki.gov.mk exposes a public
    "Отворени податоци" module with contract search and a
    "Преземи во CSV" (download CSV) export. Capture the export
    URL for signed contracts + notices from the browser's
    network tab once, set it in ESPP_CSV_URL below. Fields vary
    slightly between releases — adjust COLUMN_MAP, nothing else.
  * data.gov.mk mirrors several procurement datasets in CSV/JSON
    and can serve as a fallback source (DATA_GOV_URL).
  * Central Registry (ЦРМ): full firmographics are behind the
    paid distribution service; the beneficial-ownership register
    and the BO data attached to awarded contracts (OGP
    commitment on BO transparency in procurement) are the
    open path. Point REGISTRY_CSV at whichever export you have;
    the enrichment degrades gracefully if fields are missing.
  * LEGAL POSTURE (do not weaken): the pipeline publishes only
    facts already public, computes indicator patterns, and the
    AI prompt hard-forbids accusatory language. Every item
    carries source URLs so every claim is checkable.

  cron:  15 6 * * *  /usr/bin/python3 /opt/public-denar/pipeline.py

  env:   ANTHROPIC_API_KEY   (required for summaries)
         FEED_OUT            (default ./feed.json)
"""

import csv
import io
import json
import os
import re
import sys
import statistics
import urllib.request
from datetime import datetime, timedelta

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

ESPP_CSV_URL = os.environ.get(
    "ESPP_CSV_URL",
    # placeholder — capture the real CSV-export URL from the
    # e-nabavki Open Data module (Огласи / Склучени договори)
    "https://e-nabavki.gov.mk/OPENDATA/contracts.csv",
)
DATA_GOV_URL = os.environ.get("DATA_GOV_URL", "")      # optional fallback
REGISTRY_CSV = os.environ.get("REGISTRY_CSV", "")      # CR / BO export, optional
FEED_OUT = os.environ.get("FEED_OUT", "feed.json")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-sonnet-4-6"
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "30"))

# Map ESPP CSV headers -> canonical field names.
# Left side: canonical. Right side: candidate header substrings
# (Macedonian headers as exported by the module; adjust on first run).
COLUMN_MAP = {
    "notice_id":   ["број на оглас", "broj na oglas", "notice"],
    "authority":   ["договорен орган", "institution", "орган"],
    "subject":     ["предмет", "subject"],
    "value":       ["вредност", "цена", "value", "amount"],
    "final_value": ["конечна вредност", "реализирана", "final"],
    "contractor":  ["носител", "добитник", "економски оператор", "winner"],
    "bidders":     ["број на понуди", "понудувачи", "bids", "bidders"],
    "procedure":   ["вид на постапка", "postapka", "procedure"],
    "published":   ["датум на објав", "published"],
    "deadline":    ["краен рок", "рок за поднесување", "deadline"],
    "awarded":     ["датум на склучување", "датум на договор", "awarded", "contract date"],
    "cpv":         ["цпв", "cpv"],
    "municipality":["општина", "место", "municipality", "город"],
    "url":         ["линк", "url", "врска"],
}

# risk weights per indicator (sum capped at 100)
WEIGHTS = {
    "single_bidder": 25,
    "new_company": 20,
    "price_above": 25,      # scaled by severity
    "same_owner": 30,
    "short_ad": 15,
    "repeat_winner": 20,
    "annex_increase": 25,   # scaled by severity
    "tailored_specs": 15,   # requires notice-text heuristics
    "negotiated_no_pub": 25,
}

NEW_COMPANY_WEEKS = 26      # younger than ~6 months => flag
SHORT_AD_DAYS = 8           # notice open fewer days => flag
PRICE_DELTA_FLAG = 20       # % above CPV-category median => flag
REPEAT_WINNER_MIN = 6       # wins out of last 10 at same authority


# ------------------------------------------------------------
# 1. EXTRACT
# ------------------------------------------------------------

def http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "PublicDenar/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def normalize_headers(fieldnames):
    """Resolve real CSV headers to canonical names via COLUMN_MAP."""
    resolved = {}
    for canon, candidates in COLUMN_MAP.items():
        for h in fieldnames or []:
            hl = h.strip().lower()
            if any(c in hl for c in candidates):
                resolved[canon] = h
                break
    return resolved


def parse_number(s):
    if s is None:
        return None
    s = re.sub(r"[^\d,\.]", "", str(s))
    s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 else s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def parse_date(s):
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y %H:%M"):
        try:
            return datetime.strptime(str(s).strip()[:16], fmt)
        except (ValueError, TypeError):
            continue
    return None


def fetch_contracts():
    """Pull recent contracts from ESPP open data (CSV)."""
    raw = None
    for url in [ESPP_CSV_URL, DATA_GOV_URL]:
        if not url:
            continue
        try:
            raw = http_get(url)
            break
        except Exception as e:
            print(f"[extract] {url} failed: {e}", file=sys.stderr)
    if raw is None:
        sys.exit("[extract] no data source reachable — aborting run")

    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    cols = normalize_headers(reader.fieldnames)
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)

    rows = []
    for r in reader:
        g = lambda k: r.get(cols.get(k, ""), "").strip()
        awarded = parse_date(g("awarded"))
        if awarded and awarded < cutoff:
            continue
        rows.append({
            "id": g("notice_id") or f"ЕСЈН-{len(rows):05d}",
            "authority": g("authority"),
            "municipality": g("municipality") or g("authority"),
            "subject": g("subject"),
            "value": parse_number(g("value")) or 0,
            "final_value": parse_number(g("final_value")),
            "contractor": g("contractor"),
            "bidders": int(parse_number(g("bidders")) or 0),
            "procedure": g("procedure"),
            "published": parse_date(g("published")),
            "deadline": parse_date(g("deadline")),
            "awarded": awarded,
            "cpv": (g("cpv") or "")[:8],
            "url": g("url"),
        })
    print(f"[extract] {len(rows)} contracts in window")
    return rows


# ------------------------------------------------------------
# 2. ENRICH — Central Registry / beneficial ownership
# ------------------------------------------------------------

def load_registry():
    """
    Expected columns (any subset): company_name, embs,
    registered_date, founders, beneficial_owner, address.
    Sources: CR distribution export, BO register export, or the
    BO data attached to contract notices per the OGP commitment.
    """
    if not REGISTRY_CSV:
        print("[enrich] no registry export configured — company-age and "
              "ownership flags will be skipped")
        return {}
    try:
        raw = http_get(REGISTRY_CSV) if REGISTRY_CSV.startswith("http") \
            else open(REGISTRY_CSV, "rb").read()
    except Exception as e:
        print(f"[enrich] registry load failed: {e}", file=sys.stderr)
        return {}
    reg = {}
    for r in csv.DictReader(io.StringIO(raw.decode("utf-8-sig", "replace"))):
        name = (r.get("company_name") or "").strip().lower()
        if name:
            reg[name] = {
                "registered": parse_date(r.get("registered_date")),
                "founders": [f.strip().lower() for f in
                             re.split(r"[;,]", r.get("founders") or "") if f.strip()],
                "owner": (r.get("beneficial_owner") or "").strip().lower(),
                "address": (r.get("address") or "").strip().lower(),
            }
    print(f"[enrich] registry entries: {len(reg)}")
    return reg


# ------------------------------------------------------------
# 3. DETECT — red-flag engine
# ------------------------------------------------------------

def category_medians(rows):
    """Median contract value per CPV prefix, for price benchmarking."""
    buckets = {}
    for r in rows:
        if r["value"] > 0:
            buckets.setdefault(r["cpv"][:4] or "none", []).append(r["value"])
    return {k: statistics.median(v) for k, v in buckets.items() if len(v) >= 5}


def winner_history(rows):
    """wins per (authority, contractor) — proxy for repeat-winner."""
    h = {}
    for r in rows:
        key = (r["authority"], r["contractor"])
        h[key] = h.get(key, 0) + 1
    return h


def detect_flags(row, medians, history, registry):
    flags = []

    if row["bidders"] == 1:
        flags.append({"id": "single_bidder"})

    proc = row["procedure"].lower()
    if "преговар" in proc or "negoti" in proc:
        flags.append({"id": "negotiated_no_pub"})

    if row["published"] and row["deadline"]:
        days = (row["deadline"] - row["published"]).days
        if 0 < days < SHORT_AD_DAYS:
            flags.append({"id": "short_ad", "p": days})

    med = medians.get(row["cpv"][:4])
    if med and row["value"] > med:
        delta = round((row["value"] - med) / med * 100)
        if delta >= PRICE_DELTA_FLAG:
            flags.append({"id": "price_above", "p": delta})

    if row["final_value"] and row["value"] and row["final_value"] > row["value"]:
        inc = round((row["final_value"] - row["value"]) / row["value"] * 100)
        if inc >= 10:
            flags.append({"id": "annex_increase", "p": inc})

    wins = history.get((row["authority"], row["contractor"]), 0)
    if wins >= REPEAT_WINNER_MIN:
        flags.append({"id": "repeat_winner", "p": min(wins, 10)})

    reg = registry.get(row["contractor"].strip().lower())
    if reg and reg["registered"] and row["awarded"]:
        weeks = (row["awarded"] - reg["registered"]).days // 7
        if 0 <= weeks < NEW_COMPANY_WEEKS:
            flags.append({"id": "new_company", "p": weeks})
    # same_owner requires per-tender bidder lists + BO data:
    # when the BO-in-procurement dataset is wired in, compare
    # founders/owner/address across bidders of the same notice
    # and append {"id": "same_owner"} on a match.

    return flags


def risk_score(flags):
    score = 0
    for f in flags:
        w = WEIGHTS.get(f["id"], 10)
        if f["id"] in ("price_above", "annex_increase"):
            w = min(w + max(0, (f.get("p", 0) - 20)) // 5, w + 10)
        score += w
    return min(score, 100) if flags else max(5, min(score, 100))


# ------------------------------------------------------------
# 4. EXPLAIN — AI summaries (neutral, trilingual)
# ------------------------------------------------------------

def claude(prompt: str) -> str:
    body = json.dumps({
        "model": MODEL,
        "max_tokens": 1000,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()
    req = urllib.request.Request(
        ANTHROPIC_URL, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        })
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.loads(r.read())
    return "".join(b.get("text", "") for b in data.get("content", [])).strip()


def ai_package(item) -> dict:
    """One call per contract: trilingual subject + trilingual summary."""
    prompt = (
        "You write for a civic procurement-transparency app in North "
        "Macedonia. From the JSON below produce STRICT JSON only:\n"
        '{"subject":{"mk":"…","sq":"…","en":"…"},'
        '"summary":{"mk":"…","sq":"…","en":"…"}}\n'
        "subject: a short noun phrase naming what was bought.\n"
        "summary: 2–4 sentences per language, plain words, describing the "
        "contract facts and any flagged patterns. HARD RULES: describe "
        "patterns, never intent; no accusations, no words like corruption, "
        "fraud, guilty; state numbers exactly as given; if flags is empty, "
        "say no risk indicators were recorded. No markdown, no preamble.\n\n"
        + json.dumps(item, ensure_ascii=False, default=str)
    )
    txt = claude(prompt)
    txt = re.sub(r"^```(json)?|```$", "", txt.strip(), flags=re.M).strip()
    return json.loads(txt)


# ------------------------------------------------------------
# 5. PUBLISH
# ------------------------------------------------------------

def main():
    rows = fetch_contracts()
    registry = load_registry()
    medians = category_medians(rows)
    history = winner_history(rows)

    feed = []
    for row in rows:
        flags = detect_flags(row, medians, history, registry)
        item = {
            "id": row["id"],
            "authority": row["authority"],
            "value": int(row["value"]),
            "contractor": row["contractor"],
            "bidders": row["bidders"],
            "procedure": "neg" if any(f["id"] == "negotiated_no_pub" for f in flags) else "open",
            "published": row["published"].strftime("%d.%m.%Y") if row["published"] else "",
            "awarded": row["awarded"].strftime("%d.%m.%Y") if row["awarded"] else "",
            "flags": flags,
            "risk": risk_score(flags),
            "source_url": row["url"],
        }
        try:
            pkg = ai_package({**item, "raw_subject": row["subject"],
                              "municipality": row["municipality"]})
            item["subject"] = pkg["subject"]
            item["summary"] = pkg["summary"]
        except Exception as e:
            print(f"[explain] AI failed for {item['id']}: {e}", file=sys.stderr)
            item["subject"] = {"mk": row["subject"], "sq": row["subject"],
                               "en": row["subject"]}
        item["muni"] = {"mk": row["municipality"], "sq": row["municipality"],
                        "en": row["municipality"]}
        feed.append(item)

    feed.sort(key=lambda x: -x["risk"])
    with open(FEED_OUT, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=1)
    print(f"[publish] {len(feed)} items → {FEED_OUT} "
          f"({sum(1 for i in feed if i['risk'] >= 70)} high-risk)")


if __name__ == "__main__":
    main()
