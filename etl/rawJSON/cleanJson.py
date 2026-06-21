"""
email_extraction.py  —  config-free receipt extractor
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

No vendor configs. No CSS selectors. No LLM calls.

Reads ANY billing receipt email and extracts structured data using
four universal heuristics that work across all HTML templates:

  1. TOTAL AMOUNT
       a) Table row labelled with a "total" keyword  (most reliable)
       b) Largest font-size element containing an amount
       c) Last amount in a two-column summary table

  2. TRANSACTION / ORDER ID
       Regex scan for "order id:", "payment id:", "ride id:", etc.

  3. LINE ITEMS
       a) Two-column table rows: label | ₹ amount
       b) Text nodes matching "N × Item description"
       Deduplication + summary-row separation built in.

  4. KEY-VALUE METADATA
       All two-column table rows where the right column is NOT an amount
       (dates, statuses, names, UPI handles, etc.)

Usage
─────
    from email_extraction import extract_receipt_auto

    record = json.loads(s3.get_object(...)["Body"].read())
    result = extract_receipt_auto(record)

record  — the dict GmailSync._save() writes to S3 / ADLS:
    { message_id, account_id, sender, subject, date,
      body_html, body_text, label_ids, snippet }

result  — clean Silver-layer dict:
    { message_id, account_id, sender, subject, email_date,
      vendor, category, currency,
      total_amount, line_items, metadata,
      template_hash, confidence, extracted_at, extraction_status,
      _errors }
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import re
import json
import hashlib
from datetime import datetime, timezone
from typing import Optional
from bs4 import BeautifulSoup


# ─────────────────────────────────────────────────────────────────────────────
# COMPILED PATTERNS  (compiled once at import, reused on every call)
# ─────────────────────────────────────────────────────────────────────────────

# Matches ₹ 1,234.56  or  ₹1234  (rupee symbol variants)
_AMOUNT_RE = re.compile(r'[₹\u20b9]\s*([\d,]+\.?\d*)')

# Label text that signals a grand total row
_TOTAL_LABEL_RE = re.compile(
    r'\b(grand\s*total|total\s*amount|amount\s*paid(?:\s*\(.*?\))?'
    r'|total\s*paid|total|paid\s*successfully)\b',
    re.I
)

# Label text for summary/fee rows — excluded from line_items
_SUMMARY_LABEL_RE = re.compile(
    r'^(total|grand|subtotal|tax|gst|fee|delivery|handling|tip|discount|surcharge|item\s+bill)',
    re.I
)

# Transaction / order / ride ID patterns
_ID_RE = re.compile(
    r'\b(?:order|transaction|payment|ride|receipt|invoice|txn)\s*'
    r'(?:id|#|no\.?|number)[:\s#]*([A-Z0-9_\-]{4,40})',
    re.I
)

# "N × Item name" — line item with explicit quantity
_QTY_ITEM_RE = re.compile(r'^(\d+)\s*[xX×]\s+(.+)$')

# Vendor / category inference
_CATEGORY_SIGNALS = {
    "ride_hailing":    re.compile(r'ride|trip|driver|pickup|drop|fare', re.I),
    "grocery":         re.compile(r'grocery|instamart|blinkit|zepto|bigbasket|dunzo|deliver', re.I),
    "food_delivery":   re.compile(r'order from|restaurant|zomato|swiggy food|menu', re.I),
    "subscription":    re.compile(r'membership|subscription|plan valid|plan tenure|renew', re.I),
    "payment_gateway": re.compile(r'razorpay|paytm|payment gateway|payment successful for', re.I),
}


# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _clean_amount(text: str) -> Optional[float]:
    """Parse a float from any Indian receipt price string."""
    if not text:
        return None
    text = (text
            .replace("\u20b9", "").replace("₹", "")
            .replace("Rs.", "").replace("INR", "")
            .replace("\xa0", " ").replace("/-", "")
            .strip())
    m = re.search(r"[\d,]+\.?\d*", text)
    return float(m.group().replace(",", "")) if m else None


def _template_hash(html: str) -> str:
    """SHA-256 of the tag+class skeleton — for drift detection."""
    soup = BeautifulSoup(html, "html.parser")
    skeleton = " ".join(
        f"{t.name}.{'.'.join(sorted(t.get('class', [])))}"
        for t in soup.find_all(True)
    )
    return hashlib.sha256(skeleton.encode()).hexdigest()


def _unwrap_forwarded(html: str) -> str:
    """
    Gmail wraps forwarded emails in .gmail_quote.
    Extract its contents so heuristics run on the actual receipt HTML.
    """
    soup = BeautifulSoup(html, "html.parser")
    quote = soup.find(class_="gmail_quote")
    return str(quote) if quote else html


def _get_leaf_tds(soup) -> list:
    """Return <td> elements that contain no child <td> elements."""
    return [td for td in soup.find_all("td") if not td.find("td")]


def _infer_category(text: str) -> str:
    """Guess receipt category from full email text."""
    for category, pattern in _CATEGORY_SIGNALS.items():
        if pattern.search(text):
            return category
    return "other"


def _infer_vendor(soup, sender: str) -> str:
    """
    Best-effort vendor name:
      1. Domain from sender address
      2. First <title> tag
      3. Largest heading (h1/h2)
    """
    # From sender domain
    m = re.search(r'[\w.+-]+@([\w.-]+)', sender or "")
    if m:
        domain = m.group(1).lower()
        # Map known domains to clean names
        domain_map = {
            "rapido.bike":  "Rapido",
            "uber.com":     "Uber",
            "instamart.in": "Instamart",
            "razorpay.com": "Razorpay",
            "swiggy.in":    "Swiggy",
            "zomato.com":   "Zomato",
        }
        for key, name in domain_map.items():
            if key in domain:
                return name
        # Fallback: first part of domain, capitalised
        parts = domain.replace("no-reply.", "").replace("noreply.", "").split(".")
        if parts:
            return parts[0].capitalize()

    # From page title
    title = soup.find("title")
    if title and title.text.strip():
        return title.text.strip()[:60]

    # From first h1 or h2
    for tag in ("h1", "h2"):
        el = soup.find(tag)
        if el:
            t = el.get_text(strip=True)
            if t and not _AMOUNT_RE.search(t):
                return t[:60]

    return "Unknown"


# ─────────────────────────────────────────────────────────────────────────────
# HEURISTIC 1 — TOTAL AMOUNT
# ─────────────────────────────────────────────────────────────────────────────

def _find_total(soup) -> Optional[float]:
    """
    Three strategies in priority order:

    A) Two-column table row whose label matches a "total" keyword.
       Most reliable — vendors explicitly label their grand total.

    B) Element with the largest font-size that contains an amount.
       Works for receipt headers (Uber, Zomato, Rapido).

    C) Last amount found in any two-column table.
       Fallback for emails without font-size styling.
    """
    # ── Strategy A: total-labelled table row ──────────────────────────────────
    best_total_val  = None
    best_total_rank = 999   # lower = higher priority label

    TOTAL_RANK = {
        "grand total": 0, "grand_total": 0,
        "total amount": 1, "total_amount": 1,
        "amount paid": 2,
        "total paid": 3,
        "total": 4,
        "paid successfully": 5,
    }

    for tr in soup.find_all("tr"):
        tds = [td for td in tr.find_all("td") if not td.find("table")]
        if len(tds) != 2:
            continue
        label = tds[0].get_text(strip=True).lower()
        val_m = _AMOUNT_RE.search(tds[1].get_text())
        if not val_m:
            continue
        if _TOTAL_LABEL_RE.search(label):
            rank = next(
                (v for k, v in TOTAL_RANK.items() if k in label),
                10
            )
            if rank < best_total_rank:
                best_total_rank = rank
                best_total_val  = float(val_m.group(1).replace(",", ""))

    if best_total_val is not None:
        return best_total_val

    # ── Strategy B: largest font-size element with an amount ──────────────────
    best_size, best_val = 0.0, None
    for el in soup.find_all(True):
        style = el.get("style", "")
        fs_m  = re.search(r'font-size:\s*([\d.]+)px', style)
        if not fs_m:
            continue
        size = float(fs_m.group(1))
        if size <= best_size:
            continue
        # Use combined inner text (handles split text nodes like Zepto ₹109 + .00)
        text = el.get_text(strip=True)
        am   = _AMOUNT_RE.search(text)
        if am:
            best_size = size
            best_val  = float(am.group(1).replace(",", ""))

    if best_val is not None:
        return best_val

    # ── Strategy C: last amount in any 2-col table row ────────────────────────
    last_val = None
    for tr in soup.find_all("tr"):
        tds = [td for td in tr.find_all("td") if not td.find("table")]
        if len(tds) == 2:
            am = _AMOUNT_RE.search(tds[1].get_text())
            if am:
                last_val = float(am.group(1).replace(",", ""))
    if last_val is not None:
        return last_val

    # ── Strategy D: document-order proximity scan ─────────────────────────────
    # For div-layout emails (e.g. Rapido) with no table rows and no font-size.
    # Find the text node containing a total-label keyword, then find the nearest
    # text node (by document index) that contains an ₹ amount.
    all_nodes    = list(soup.find_all(string=True))
    amount_nodes = [(i, n.strip()) for i, n in enumerate(all_nodes) if _AMOUNT_RE.search(n.strip())]
    total_nodes  = [(i, n.strip()) for i, n in enumerate(all_nodes)
                    if _TOTAL_LABEL_RE.search(n.strip()) and len(n.strip()) < 40]

    for ti, _ in total_nodes:
        best_dist, best_amt = 999, None
        for ai, anode in amount_nodes:
            dist = abs(ai - ti)
            if dist < best_dist:
                best_dist = dist
                best_amt  = anode
        if best_amt and best_dist <= 10:
            m = _AMOUNT_RE.search(best_amt)
            if m:
                return float(m.group(1).replace(",", ""))

    return None


# ─────────────────────────────────────────────────────────────────────────────
# HEURISTIC 2 — TRANSACTION / ORDER ID
# ─────────────────────────────────────────────────────────────────────────────

def _find_transaction_id(soup) -> Optional[str]:
    """
    Scan full text for patterns like:
      "Order ID: 7608298379"
      "Payment Id pay_SY9ZVny0QFVODF"
      "Ride ID RD17803846931280855"
      "Transaction Id 234176416039497"
    """
    text = soup.get_text(" ")
    m = _ID_RE.search(text)
    return m.group(1).strip() if m else None


# ─────────────────────────────────────────────────────────────────────────────
# HEURISTIC 3 — LINE ITEMS
# ─────────────────────────────────────────────────────────────────────────────

def _find_line_items(soup) -> tuple[list, dict]:
    """
    Returns (line_items, summary_fields).

    line_items   — purchase rows: [{quantity, description, amount}]
    summary_fields — fee/total rows promoted to scalar fields:
                     {"item_bill": 205.0, "handling_fee": 7.0, ...}

    Strategy A: two-column table rows
      - Right column has an ₹ amount
      - Left column is the item label
      - Rows matching _SUMMARY_LABEL_RE → summary_fields
      - Rows matching _QTY_ITEM_RE in label → parse quantity

    Strategy B: text nodes matching "N × Item"
      - Used for emails like Zomato where items have no per-item price
    """
    line_items     = []
    summary_fields = {}
    seen           = set()

    # ── Strategy A: two-column table rows ─────────────────────────────────────
    for tr in soup.find_all("tr"):
        tds = [td for td in tr.find_all("td") if not td.find("table")]
        if len(tds) != 2:
            continue
        label      = tds[0].get_text(strip=True)
        right_text = tds[1].get_text(strip=True)
        am         = _AMOUNT_RE.search(right_text)
        if not am:
            continue

        val = float(am.group(1).replace(",", ""))

        # Total-labelled rows → skip from line_items (captured by _find_total)
        if _TOTAL_LABEL_RE.search(label):
            key = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
            summary_fields[key] = val
            continue

        # Summary / fee rows
        if _SUMMARY_LABEL_RE.search(label):
            key = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
            summary_fields[key] = val
            continue

        if not label:
            continue

        # Parse qty from label e.g. "2 x Amul Milk"
        qty  = 1
        desc = label
        qm   = _QTY_ITEM_RE.match(label)
        if qm:
            qty  = int(qm.group(1))
            desc = qm.group(2).strip()

        key = (qty, desc)
        if key in seen:
            continue
        seen.add(key)
        line_items.append({"quantity": qty, "description": desc, "amount": val})

    # ── Strategy B: "N × Item" text nodes (no per-item price) ─────────────────
    for el in soup.find_all(string=True):
        txt = el.strip()
        if not txt:
            continue
        # Only look at leaf text nodes (not inside tds that already have a table)
        parent = el.parent
        if parent and parent.find("table"):
            continue
        qm = _QTY_ITEM_RE.match(txt)
        if qm:
            qty  = int(qm.group(1))
            desc = qm.group(2).strip()
            key  = (qty, desc)
            if key not in seen:
                seen.add(key)
                line_items.append({"quantity": qty, "description": desc, "amount": None})

    return line_items, summary_fields


# ─────────────────────────────────────────────────────────────────────────────
# HEURISTIC 4 — KEY-VALUE METADATA
# ─────────────────────────────────────────────────────────────────────────────

def _find_metadata(soup) -> dict:
    """
    Collect all two-column table rows where:
      - Left column is a clean label (no amounts, not too long)
      - Right column is not a plain ₹ amount (dates, IDs, statuses, names)

    Returns a flat dict: {"Date of Email": "03-04-2026", "Plan Tenure": "3 Months", ...}
    Skips rows already captured as line_items or total.
    """
    metadata = {}
    LABEL_RE  = re.compile(r'^[A-Za-z][A-Za-z\s()&/]{1,50}$')

    for tr in soup.find_all("tr"):
        tds = [td for td in tr.find_all("td") if not td.find("table")]
        if len(tds) != 2:
            continue
        label = tds[0].get_text(strip=True)
        value = tds[1].get_text(strip=True)

        if not label or not value:
            continue
        if not LABEL_RE.match(label):
            continue
        # Skip if the value is purely an amount (those go to line_items/total)
        if _AMOUNT_RE.match(value) and len(value) < 15:
            continue
        if label in metadata:
            continue
        metadata[label] = value

    return metadata


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

def extract_receipt(record: dict) -> dict:
    """
    Config-free extraction. Runs all four heuristics and assembles the result.

    Args
        record : dict from GmailSync._save()

    Returns
        Clean Silver-layer dict.
    """
    raw_html = record.get("body_html", "")
    if not raw_html:
        raise ValueError(f"No body_html in record {record.get('message_id')}")

    # Unwrap Gmail forwarded wrapper if present
    html = _unwrap_forwarded(raw_html)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    errors = []

    # ── Run heuristics ─────────────────────────────────────────────────────────
    total_amount             = _find_total(soup)
    transaction_id           = _find_transaction_id(soup)
    line_items, summary      = _find_line_items(soup)
    metadata                 = _find_metadata(soup)
    vendor                   = _infer_vendor(soup, record.get("sender", ""))
    category                 = _infer_category(text)

    if total_amount is None:
        errors.append("total_amount not found")

    # ── Assemble result ────────────────────────────────────────────────────────
    result = {
        # identity
        "message_id":      record.get("message_id"),
        "account_id":      record.get("account_id"),
        "sender":          record.get("sender"),
        "subject":         record.get("subject"),
        "email_date":      record.get("date"),
        # inferred
        "vendor":          vendor,
        "category":        category,
        "currency":        "INR",
        # extracted
        "total_amount":    total_amount,
        "transaction_id":  transaction_id,
        "line_items":      line_items,
        "summary":         summary,
        "metadata":        metadata,
        # pipeline
        "template_hash":   _template_hash(raw_html),
        "confidence":      _confidence_score(total_amount, transaction_id, line_items),
        "extracted_at":    datetime.now(timezone.utc).isoformat(),
        "extraction_status": "extracted",
        "_errors":         errors,
    }

    if errors or result["confidence"] < 0.5:
        result["extraction_status"] = "quarantine"

    return result


def _confidence_score(total, txn_id, line_items) -> float:
    """
    Heuristic confidence: 0.0–1.0
    Penalise for missing key fields.
    """
    score = 1.0
    if total     is None:  score -= 0.4
    if txn_id    is None:  score -= 0.2
    if not line_items:     score -= 0.1
    return round(max(score, 0.0), 2)


# Alias for ETL pipeline
extract_receipt_auto = extract_receipt


# ─────────────────────────────────────────────────────────────────────────────
# TEST HARNESS
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import email as emaillib
    from email import policy as epolicy

    BASE = "/mnt/user-data/uploads/"
    TESTS = [
        ("Rapido_Invoice__1_.eml",                                        54.0,   "RD17803846931280855"),
        ("Your_Wednesday_evening_trip_with_Uber__1_.eml",                 10.0,   None),
        ("Your_Instamart_order_was_successfully_delivered__1_.eml",       212.0,  "240858319590787"),
        ("Payment_successful_for_ZEPTO_MARKETPLACE_PRIVATE_LIMITED.eml",  109.0,  "pay_SY9ZVny0QFVODF"),
        ("Mridul_Soni__you_are_now_a_Swiggy_One_member_.eml",             1.0,    "234176416039497"),
        ("Your_Zomato_order_from_Kanha.eml",                              183.0,  "7608298379"),
    ]

    def eml_to_record(path: str) -> dict:
        with open(path, "rb") as f:
            msg = emaillib.message_from_bytes(f.read(), policy=epolicy.default)
        html = text = ""
        for part in msg.walk():
            ct = part.get_content_type()
            try:
                p = part.get_payload(decode=True)
                if not p: continue
                d = p.decode(part.get_content_charset() or "utf-8", "replace")
                if ct == "text/html"  and not html: html = d
                if ct == "text/plain" and not text: text = d
            except: pass
        return {
            "message_id": path.split("/")[-1].replace(".eml", ""),
            "account_id": "test-acc-001",
            "sender":     msg.get("From", ""),
            "subject":    msg.get("Subject", ""),
            "date":       msg.get("Date", ""),
            "body_html":  html,
            "body_text":  text,
            "label_ids":  [],
            "snippet":    "",
        }

    passed = failed = 0
    print(f"\n{'VENDOR':<28} {'TOTAL':>8} {'EXP':>8} {'T_OK':>5}  {'TXN_ID':<30} {'I_OK':>5}")
    print("─" * 90)

    for fname, exp_total, exp_txn in TESTS:
        record = eml_to_record(BASE + fname)
        result = extract_receipt(record)

        got_total  = result["total_amount"]
        got_txn    = result["transaction_id"]
        items      = result["line_items"]

        total_ok = "✓" if got_total == exp_total else "✗"
        txn_ok   = "✓" if (exp_txn is None or got_txn == exp_txn) else "✗"

        vendor = result["vendor"][:27]
        print(f"{vendor:<28} {str(got_total):>8} {exp_total:>8} {total_ok:>5}  {str(got_txn):<30} {txn_ok:>5}")

        if total_ok == "✓": passed += 1
        else: failed += 1
        if txn_ok == "✓":   passed += 1
        else: failed += 1

    print("─" * 90)
    print(f"\nResult: {passed} passed, {failed} failed\n")

    # Full JSON output for one vendor
    print("=== Full result — Instamart ===")
    record = eml_to_record(BASE + "Your_Instamart_order_was_successfully_delivered__1_.eml")
    result = extract_receipt(record)
    clean  = {k: v for k, v in result.items() if not k.startswith("_")}
    print(json.dumps(clean, indent=2, ensure_ascii=False))

    sys.exit(0 if failed == 0 else 1)