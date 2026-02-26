"""
SEC EDGAR Automated Financial Data Extraction (HOOD FP&A Pipeline)

WHAT IT DOES
This end-to-end SEC automation script orchestrates financial statement and XBRL
fact extraction for building FP&A models. It combines two extraction paths:
  1. Parsed financial statements from 10-Q XLSX workbooks or FilingSummary HTML
  2. XBRL CompanyFacts for GAAP-normalized time series and derived metrics

WHY IT EXISTS
10-Q filings contain financial statements in both XLSX (Financial_Report.xlsx)
and HTML (embedded in FilingSummary.xml). Direct statement parsing captures
footnote details and precise line items. Complementing with CompanyFacts XBRL
data provides standardized, audit-quality facts and enables gap-filling
(e.g., Q4 derivation from 10-K annual data, YTD→quarterly conversion).

CORE PIPELINE STEPS
1. Ticker → CIK: Resolves ticker symbol to SEC CIK identifier via official SEC APIs,
   with fallback to manual mapping for edge cases.
2. 10-Q Accessions: Retrieves the N most recent 10-Q accession numbers and report dates
   from the SEC Submissions API.
3. Statement Extraction (run_statement_pipeline):
   - Attempts XLSX parse (Financial_Report.xlsx) for each accession
   - Falls back to FilingSummary.xml HTML statement pages on parse failure
   - Extracts three core statements (Income Statement, Balance Sheet, Cash Flow)
   - Uses regex pattern matching to find specific line items (revenue, OpEx, debt, etc.)
   - Outputs raw CSV with one row per accession
4. CompanyFacts Extraction (run_companyfacts_pipeline):
   - Fetches CompanyFacts JSON from SEC's XBRL API for all filings
   - Extracts quarterly time series for each XBRL concept (tag)
   - Handles YTD→quarterly conversion (common in 10-Q filings)
   - Derives Q4 from 10-K annual less (Q1+Q2+Q3)
   - Extracts revenue segments from XLSX note schedules
   - Creates debt proxy from broker-dealer specific tags
   - Outputs wide-format CSV (one row per line item, columns per quarter)

OUTPUTS (in current working directory / data/)
Statement-parsed CSVs (one accession per row):
  - {TICKER}_stmt_IS_{N}Q.csv   (Income Statement)
  - {TICKER}_stmt_BS_{N}Q.csv   (Balance Sheet)
  - {TICKER}_stmt_CF_{N}Q.csv   (Cash Flow)
CompanyFacts CSVs (wide format, one line per row, cols per quarter):
  - {TICKER}_companyfacts_IS_{N}Q.csv   (Income Statement with Q4 derived)
  - {TICKER}_companyfacts_BS_{N}Q.csv   (Balance Sheet with debt proxy)
  - {TICKER}_companyfacts_CF_{N}Q.csv   (Cash Flow with CFO/Capex converted to Q)
  - {TICKER}_companyfacts_tag_report.csv (XBRL tag resolution audit trail)

LINE ITEMS EXTRACTED
Income Statement:
  - Transaction-based revenue, Net interest revenue, Other revenue, Total revenue
  - Operating expenses, Stock-based compensation, Net income
Balance Sheet:
  - Cash & equivalents, Restricted cash, Receivables, Payables
  - Debt (from CompanyFacts proxy), Total Equity
  - Assets under custody (often not in statements; may be blank)
Cash Flow:
  - Net income, Stock-based compensation (YTD→Q converted)
  - Change in working capital, Capex (YTD→Q converted), CFO, Free Cash Flow

DEPENDENCIES
Install with: pip install pandas requests openpyxl lxml

RUN
  python -m src.hood_sec_extract_v3 --ticker HOOD --quarters 10

EXAMPLE
  python -m src.hood_sec_extract_v3 --ticker HOOD --quarters 20
  → Outputs HOOD_{stmt,companyfacts}_*.csv with 20 quarters of data

NOTES
- Assets under custody often appears in MD&A or supplementary schedules, not
  core financial statements. The script may produce None/NaN for this item.
- "Other revenue" regex is deliberately generic (r"\bother\b"); tighten to the
  exact statement label if it captures unwanted other income/OCI items.
- CompanyFacts debt proxy is HOOD-specific (ConvertibleDebtNoncurrent,
  SecuritiesBorrowedLiability, and other broker-dealer liability tags).
  Adapt for other tickers.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from lxml import etree

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module Configuration: Paths & HTTP Settings
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_DIR, REPO_ROOT, TICKER

DATA_DIR.mkdir(exist_ok=True)


# SEC API HTTP Configuration
# (User-Agent required per SEC EDGAR robots.txt; identifies script + contact)
# Set SEC_USER_AGENT env variable with your name and email, e.g.:
#   export SEC_USER_AGENT="Your Name your@email.com"
USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "HOOD-Financial-Pipeline github.com/angel-rios/hood-financial-pipeline",
)
HEADERS_JSON = {
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json,text/html,*/*",
}
HEADERS_BYTES = {
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "*/*",
}
TIMEOUT_S = 45
PAUSE_S = 0.35


# ---------------------------------------------------------------------------
# HTTP Request Helpers (with retry & rate-limiting)
# ---------------------------------------------------------------------------

def _retry_sleep(attempt: int) -> None:
    """Exponential backoff: sleep proportional to attempt number, capped at 30s."""
    time.sleep(min(1.25 * (2 ** attempt), 30.0))


def get_json(url: str) -> dict:
    """Fetch url and return its body parsed as JSON.

    Retries up to six times with exponential back-off on 429/503 responses or
    transient request errors; raises RuntimeError if all attempts fail.

    This function respects SEC API rate limits (429 Too Many Requests) and
    server errors (503 Service Unavailable) by waiting before retry.
    """
    last_err = None
    for attempt in range(1, 7):
        try:
            r = requests.get(url, headers=HEADERS_JSON, timeout=TIMEOUT_S)
            if r.status_code in (429, 503):
                _retry_sleep(attempt)
                continue
            r.raise_for_status()
            time.sleep(PAUSE_S)
            return r.json()
        except requests.exceptions.RequestException as e:
            last_err = e
            time.sleep(1.0 * attempt)
    raise RuntimeError(f"Failed GET JSON {url}. Last error: {last_err}")


def get_text(url: str) -> str:
    """Fetch url and return the response body decoded as a string.

    Uses the same retry/back-off strategy as get_json but returns r.text
    instead of parsed JSON; raises RuntimeError if all six attempts fail.

    Used for HTML and XML content (FilingSummary.xml, statement HTML pages).
    """
    last_err = None
    for attempt in range(1, 7):
        try:
            r = requests.get(url, headers=HEADERS_BYTES, timeout=TIMEOUT_S)
            if r.status_code in (429, 503):
                _retry_sleep(attempt)
                continue
            r.raise_for_status()
            time.sleep(PAUSE_S)
            return r.text
        except requests.exceptions.RequestException as e:
            last_err = e
            time.sleep(1.0 * attempt)
    raise RuntimeError(f"Failed GET text {url}. Last error: {last_err}")


def get_bytes(url: str) -> bytes:
    """Fetch url and return the raw response body as bytes.

    Useful for binary resources such as Excel workbooks; applies the same
    retry/back-off logic as get_json and raises RuntimeError after six failures.
    """
    last_err = None
    for attempt in range(1, 7):
        try:
            r = requests.get(url, headers=HEADERS_BYTES, timeout=TIMEOUT_S)
            if r.status_code in (429, 503):
                _retry_sleep(attempt)
                continue
            r.raise_for_status()
            time.sleep(PAUSE_S)
            return r.content
        except requests.exceptions.RequestException as e:
            last_err = e
            time.sleep(1.0 * attempt)
    raise RuntimeError(f"Failed GET bytes {url}. Last error: {last_err}")


def download_file(url: str, out_path: str) -> None:
    """Download a file from url and save to out_path with directory creation."""
    data = get_bytes(url)
    dirname = os.path.dirname(out_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(data)


# ---------------------------------------------------------------------------
# Ticker-to-CIK Resolution
# ---------------------------------------------------------------------------

def get_cik_from_ticker(ticker: str) -> str:
    """Resolve a stock ticker symbol to its SEC CIK identifier.

    Attempts official SEC API endpoints:
      1. company_tickers_exchange.json (preferred, includes exchange info)
      2. company_tickers.json (fallback)

    Falls back to a hardcoded manual map for tickers not in official APIs.

    Args
    ----
    ticker : str
        Stock symbol (e.g., "HOOD"). Case-insensitive.

    Returns
    -------
    str
        10-digit zero-padded CIK (e.g., "0001783879" for HOOD).

    Raises
    ------
    ValueError
        If CIK cannot be resolved from API or manual map.
    """
    t = ticker.upper().strip()

    # Manual fallback for tickers not yet in SEC APIs or known edge cases.
    # Update this map to add custom tickers.
    manual = {
        "HOOD": "0001783879",
    }

    mapping_urls = [
        "https://www.sec.gov/files/company_tickers_exchange.json",
        "https://www.sec.gov/files/company_tickers.json",
    ]

    for url in mapping_urls:
        try:
            data = get_json(url)
            # First format: data["data"] is a list of [cik, ticker, ...] rows
            if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                for row in data["data"]:
                    if len(row) >= 2 and str(row[1]).upper() == t:
                        return str(row[0]).zfill(10)
            # Second format: data is a dict of {index: {ticker, cik_str, ...}}
            if isinstance(data, dict) and all(isinstance(v, dict) for v in data.values()):
                for _, rec in data.items():
                    if str(rec.get("ticker", "")).upper() == t:
                        return str(rec["cik_str"]).zfill(10)
        except (ValueError, KeyError):
            continue

    if t in manual:
        return manual[t]

    raise ValueError(f"Could not resolve CIK for ticker '{t}'. Add it to manual map.")


# ---------------------------------------------------------------------------
# SEC Submissions API: Recent 10-Q Filings
# ---------------------------------------------------------------------------

def get_last_10q_accessions(cik10: str, n: int) -> List[Tuple[str, str]]:
    """Retrieve the N most recent 10-Q accession numbers and report dates.

    Args
    ----
    cik10 : str
        10-digit zero-padded CIK.
    n : int
        Number of recent 10-Q filings to retrieve.

    Returns
    -------
    List[Tuple[str, str]]
        List of (accession_number, report_date) tuples, ordered most-recent-first.
        accession_number is the SEC's unique filing ID (e.g., "0001193125-25-000001").
        report_date is ISO format (e.g., "2024-12-31").

    Raises
    ------
    RuntimeError
        If no 10-Q filings are found in the submissions list.
    """
    sub = get_json(f"https://data.sec.gov/submissions/CIK{cik10}.json")
    recent = sub.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accns = recent.get("accessionNumber", [])
    report_dates = recent.get("reportDate", [])

    out = []
    for form, accn, rdate in zip(forms, accns, report_dates):
        if form == "10-Q":
            out.append((accn, rdate))
        if len(out) >= n:
            break
    if not out:
        raise RuntimeError("No 10-Q filings found in recent submissions list.")
    return out


# ---------------------------------------------------------------------------
# SEC EDGAR Archive Helpers: File Listing & Locating
# ---------------------------------------------------------------------------

def accession_base(cik10: str, accn: str) -> str:
    """Construct the base URL for an EDGAR accession.

    Args
    ----
    cik10 : str
        10-digit CIK.
    accn : str
        Accession number (e.g., "0001193125-25-000001").

    Returns
    -------
    str
        EDGAR archive base URL (without trailing slash).

    Example::

        accession_base("0001783879", "0001193125-25-000001")
        # → 'https://www.sec.gov/Archives/edgar/data/1783879/000119312525000001'
    """
    cik_int = str(int(cik10))
    accn_nodash = accn.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_nodash}"


def list_accession_files(cik10: str, accn: str) -> List[str]:
    """List all file names in an EDGAR accession directory.

    Uses the EDGAR index.json file to retrieve the directory listing.

    Args
    ----
    cik10 : str
        10-digit CIK.
    accn : str
        Accession number.

    Returns
    -------
    List[str]
        List of file names (without directory path).
    """
    idx = get_json(f"{accession_base(cik10, accn)}/index.json")
    items = idx.get("directory", {}).get("item", [])
    return [it.get("name", "") for it in items if it.get("name")]


def find_best_xlsx(cik10: str, accn: str) -> Optional[str]:
    """Locate the Financial_Report.xlsx (or best XLSX substitute) in an accession.

    Heuristic ranking:
      1. financial_report.xlsx (exact match, case-insensitive)
      2. Any .xlsx file with "financial" or "report" in name
      3. Any other .xlsx file (fallback)

    Args
    ----
    cik10 : str
        10-digit CIK.
    accn : str
        Accession number.

    Returns
    -------
    Optional[str]
        Full EDGAR URL to the XLSX file, or None if not found.
    """
    base = accession_base(cik10, accn)
    files = list_accession_files(cik10, accn)
    lower = {f.lower(): f for f in files}

    # Preferred: exact financial_report.xlsx
    if "financial_report.xlsx" in lower:
        return f"{base}/{lower['financial_report.xlsx']}"

    # Fallback: any XLSX with "financial" or "report" in name, ranked by length (shorter = more focused)
    candidates = [f for f in files if f.lower().endswith(".xlsx")]
    if candidates:
        ranked = sorted(
            candidates,
            key=lambda x: (
                0 if ("financial" in x.lower() or "report" in x.lower()) else 1,
                len(x),
            ),
        )
        return f"{base}/{ranked[0]}"
    return None


def find_filing_summary_url(cik10: str, accn: str) -> Optional[str]:
    """Locate FilingSummary.xml in an accession.

    FilingSummary.xml is the SEC's standard XBRL index file. It contains
    metadata and URLs for HTML statement pages embedded in the XBRL filing.

    Args
    ----
    cik10 : str
        10-digit CIK.
    accn : str
        Accession number.

    Returns
    -------
    Optional[str]
        Full EDGAR URL to FilingSummary.xml, or None if not found.
    """
    base = accession_base(cik10, accn)
    files = list_accession_files(cik10, accn)
    for f in files:
        if f.lower() == "filingsummary.xml":
            return f"{base}/{f}"
    return None


# ---------------------------------------------------------------------------
# Statement Parsing: XLSX and HTML Extraction
# ---------------------------------------------------------------------------

def normalize(s: str) -> str:
    """Lowercase s, collapse runs of whitespace to a single space, and strip.

    Used for case-insensitive line-item matching across different filing formats.

    Examples::

        normalize("  Net    Income  ")    # → 'net income'
        normalize("CASH & CASH EQUIVALENTS")  # → 'cash & cash equivalents'
    """
    return re.sub(r"\s+", " ", str(s)).strip().lower()


def best_sheet_name(sheet_names: List[str], kind: str) -> Optional[str]:
    """Select the most likely sheet name for a statement type from an Excel workbook.

    Scoring heuristic ranks sheet names by relevance to statement type (IS/BS/CF),
    with penalties for notes, schedules, and other non-statement content.

    Args
    ----
    sheet_names : List[str]
        All sheet names in the workbook.
    kind : str
        Statement type: "is" (income statement), "bs" (balance sheet), "cf" (cash flow).

    Returns
    -------
    Optional[str]
        Best matching sheet name, or None if no sheet scores > 0.

    Example::

        sheets = ["Consolidated Statements of Operations", "Notes to Financials", "Data"]
        best_sheet_name(sheets, "is")
        # → 'Consolidated Statements of Operations'
    """
    sn = [(sh, normalize(sh)) for sh in sheet_names]
    negatives = {"is": ["income taxes"], "bs": [], "cf": []}

    if kind == "is":
        strong = ["statements of operations", "statement of operations", "consolidated statements of operations"]
        medium = ["operations", "income statement", "statements of income"]
        weak = ["income"]
    elif kind == "bs":
        strong = ["balance sheets", "balance sheet", "consolidated balance sheets"]
        medium = ["financial position"]
        weak = ["balance"]
    else:
        strong = ["statements of cash flows", "statement of cash flows", "consolidated statements of cash flows"]
        medium = ["cash flows", "cash flow"]
        weak = ["cash"]

    def score(name_n: str) -> int:
        """Score a normalized sheet name; higher is better."""
        for neg in negatives.get(kind, []):
            if neg in name_n:
                return -10_000
        s = 0
        if any(k in name_n for k in strong):
            s += 100
        if any(k in name_n for k in medium):
            s += 30
        if any(k in name_n for k in weak):
            s += 5
        if "statement" in name_n or "statements" in name_n:
            s += 20
        if "consolidated" in name_n:
            s += 5
        if "note" in name_n or "notes" in name_n or "schedule" in name_n:
            s -= 20
        return s

    ranked = sorted(sn, key=lambda x: score(x[1]), reverse=True)
    best = ranked[0][0] if ranked and score(ranked[0][1]) > 0 else None
    return best


def extract_statement_from_xlsx(xlsx_path: str, kind: str) -> pd.DataFrame:
    """Extract a financial statement (IS/BS/CF) from an Excel workbook.

    Strategy: Identify the best sheet by name heuristic, then search for
    label (column 0) and numeric value (first column with 5+ values) columns.
    Normalizes line items for cross-filing consistency.

    Args
    ----
    xlsx_path : str
        Path to Financial_Report.xlsx.
    kind : str
        Statement type: "is", "bs", or "cf".

    Returns
    -------
    pd.DataFrame
        Columns: line_item (raw), line_item_n (normalized), value (float).

    Raises
    ------
    RuntimeError
        If no usable sheet or numeric column is found.

    Notes
    -----
    Many 10-Q XLSXs have inconsistent formatting. This function is resilient
    to missing headers and floating-point noise (parse errors → NaN).
    """
    last_err: Optional[Exception] = None
    with pd.ExcelFile(xlsx_path) as xl:
        candidates: List[str] = []
        best = best_sheet_name(xl.sheet_names, kind)
        if best:
            candidates.append(best)
        for sh in xl.sheet_names:
            if sh not in candidates:
                candidates.append(sh)

        for sh in candidates:
            try:
                # Use xl.parse() to avoid re-opening the file for each sheet
                df = xl.parse(sh, header=None)
                df = df.dropna(how="all").reset_index(drop=True)

                label_col = 0
                value_col = None
                # Search columns 1–9 for the first with 5+ numeric values
                for c in range(1, min(10, df.shape[1])):
                    col = pd.to_numeric(df.iloc[:, c], errors="coerce")
                    if col.notna().sum() >= 5:
                        value_col = c
                        break
                if value_col is None:
                    raise RuntimeError(f"No numeric value column in sheet={sh}")

                out = pd.DataFrame(
                    {
                        "line_item": df.iloc[:, label_col].astype(str),
                        "value": pd.to_numeric(df.iloc[:, value_col], errors="coerce"),
                    }
                )
                out["line_item_n"] = out["line_item"].map(normalize)
                out = out[out["line_item_n"].ne("")].copy()

                if out["value"].notna().sum() < 5:
                    raise RuntimeError(f"Too few numeric values in sheet={sh}")

                return out
            except (KeyError, IndexError, RuntimeError) as e:
                last_err = e
                continue

    raise RuntimeError(f"Failed to parse usable statement from {xlsx_path} kind={kind}. Last error: {last_err}")


def parse_statement_html(url: str) -> pd.DataFrame:
    """Parse an HTML financial statement page and return a normalized (line_item, value) frame.

    Used as fallback when XLSX is unavailable. Pandas pd.read_html() extracts tables,
    then we identify label (column 0) and value (best numeric) columns.

    Args
    ----
    url : str
        Full EDGAR URL to an R*.htm statement page.

    Returns
    -------
    pd.DataFrame
        Columns: line_item (raw), line_item_n (normalized), value (float).

    Raises
    ------
    RuntimeError
        If no usable table or numeric data is found — callers should
        not silently swallow this; a missing statement is a data-quality issue.
    """
    html_bytes = get_bytes(url)
    tables = pd.read_html(html_bytes)
    if not tables:
        raise RuntimeError(f"No HTML tables found at {url}")

    # Pick the largest table by cell count
    t = max(tables, key=lambda x: x.shape[0] * x.shape[1])
    t = t.dropna(how="all").reset_index(drop=True)

    label_col = 0
    best_val_col = None
    best_count = -1
    # Search columns 1+ for the one with most numeric values
    for c in range(1, t.shape[1]):
        cnt = pd.to_numeric(t.iloc[:, c], errors="coerce").notna().sum()
        if cnt > best_count:
            best_count = cnt
            best_val_col = c

    if best_val_col is None or best_count < 3:
        raise RuntimeError(
            f"No usable numeric column found in HTML statement at {url} "
            f"(best numeric count={best_count}). The page may have changed structure."
        )

    out = pd.DataFrame(
        {
            "line_item": t.iloc[:, label_col].astype(str),
            "value": pd.to_numeric(t.iloc[:, best_val_col], errors="coerce"),
        }
    )
    out["line_item_n"] = out["line_item"].map(normalize)
    out = out[out["line_item_n"].ne("")].copy()

    if len(out) < 3:
        raise RuntimeError(
            f"Parsed only {len(out)} rows from HTML statement at {url}. "
            "Extraction likely failed — check if the HTML structure has changed."
        )
    return out


def filing_summary_statement_urls(cik10: str, accn: str) -> Dict[str, str]:
    """Extract statement page URLs from FilingSummary.xml.

    Parses the XBRL metadata to locate Income Statement, Balance Sheet, and
    Cash Flow Statement HTML pages. Returns a dict keyed by statement kind.

    Args
    ----
    cik10 : str
        10-digit CIK.
    accn : str
        Accession number.

    Returns
    -------
    Dict[str, str]
        {"is": url, "bs": url, "cf": url} with statement URLs.
        Missing keys are omitted.
    """
    fs_url = find_filing_summary_url(cik10, accn)
    if not fs_url:
        return {}

    xml_txt = get_text(fs_url)
    root = etree.fromstring(xml_txt.encode("utf-8", errors="ignore"))
    base = accession_base(cik10, accn)

    reports = root.xpath(".//Report")
    found: Dict[str, str] = {}

    def score(kind: str, name: str) -> bool:
        """Check if a report name matches the statement kind."""
        n = normalize(name)
        if kind == "is":
            return ("operations" in n) or ("income" in n)
        if kind == "bs":
            return ("balance sheet" in n) or ("financial position" in n)
        if kind == "cf":
            return ("cash flows" in n) or ("cash flow" in n)
        return False

    for rep in reports:
        short = rep.findtext("ShortName") or ""
        longn = rep.findtext("LongName") or ""
        html = rep.findtext("HtmlFileName") or ""
        if not html:
            continue

        name = f"{short} {longn}"
        if "is" not in found and score("is", name):
            found["is"] = f"{base}/{html}"
        if "bs" not in found and score("bs", name):
            found["bs"] = f"{base}/{html}"
        if "cf" not in found and score("cf", name):
            found["cf"] = f"{base}/{html}"

        if len(found) == 3:
            break

    return found


# ---------------------------------------------------------------------------
# Line Item Matching from Parsed Statements
# ---------------------------------------------------------------------------

def pick_lines(stmt: pd.DataFrame, wanted: Dict[str, List[str]]) -> Dict[str, Optional[float]]:
    """Extract specific line items from a statement using regex patterns.

    Searches the statement for line items matching each regex pattern in `wanted`,
    and returns the first matching numeric value for each output line.

    Args
    ----
    stmt : pd.DataFrame
        Parsed statement with columns: line_item, line_item_n, value.
    wanted : Dict[str, List[str]]
        {"Output Line Name": [r"regex1", r"regex2", ...], ...}
        Patterns are matched against the raw line_item column.

    Returns
    -------
    Dict[str, Optional[float]]
        {"Output Line Name": value_or_None, ...}

    Example::

        wanted = {"Revenue": [r"\\btotal\\b.*\\brevenue", r"\\bsales\\b"]}
        pick_lines(stmt, wanted)
        # → {"Revenue": 1234567.89}
    """
    res: Dict[str, Optional[float]] = {}
    for out_line, pats in wanted.items():
        found_val = None
        for pat in pats:
            rx = re.compile(pat, re.IGNORECASE)
            hits = stmt[stmt["line_item"].str.contains(rx, na=False)]
            if not hits.empty:
                v = hits["value"].dropna()
                if not v.empty:
                    found_val = float(v.iloc[0])
                    break
        res[out_line] = found_val
    return res


# ---------------------------------------------------------------------------
# CompanyFacts (XBRL Concepts) Extraction Helpers
# ---------------------------------------------------------------------------

def get_companyfacts(cik10: str) -> dict:
    """Fetch the full CompanyFacts JSON from the SEC XBRL API.

    CompanyFacts contains all XBRL facts (accounting concepts) filed by a company
    across all periods and forms. Each fact has multiple observations (one per
    filing period, possibly with different units and form types).

    Args
    ----
    cik10 : str
        10-digit CIK.

    Returns
    -------
    dict
        Full CompanyFacts JSON structure.

    Notes
    -----
    The API response is typically 5–50 MB and can take several seconds to download.
    """
    return get_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json")


def search_tags(companyfacts: dict, pattern: str, taxonomy: str = "us-gaap") -> List[str]:
    """Search CompanyFacts for XBRL tags matching a regex pattern.

    Useful for discovering available concepts when exact tag name is unknown.

    Args
    ----
    companyfacts : dict
        CompanyFacts JSON from get_companyfacts().
    pattern : str
        Regex pattern to match against tag names.
    taxonomy : str
        Namespace ("us-gaap" or "ifrs-full").

    Returns
    -------
    List[str]
        Sorted list of matching tag names.

    Example::

        tags = search_tags(facts, r"Debt|Borrowing")
        # → ['ConvertibleDebtNoncurrent', 'LongTermDebt', ...]
    """
    rx = re.compile(pattern, re.IGNORECASE)
    facts = companyfacts.get("facts", {}).get(taxonomy, {})
    return sorted([tag for tag in facts.keys() if rx.search(tag)])


def extract_quarterly_fact(
    companyfacts: dict,
    tag: str,
    taxonomy: str = "us-gaap",
    unit: str = "USD",
    forms: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Extract a time series of quarterly values for a single XBRL concept.

    Filters to quarterly filings only (fp in {Q1, Q2, Q3, Q4}) and deduplicates
    by period end date (keeping most recent filing).

    Args
    ----
    companyfacts : dict
        Full JSON dict returned by the SEC CompanyFacts API.
    tag : str
        XBRL concept name (e.g., "Revenues", "NetIncomeLoss").
    taxonomy : str
        Fact namespace ("us-gaap" by default).
    unit : str
        Unit key (e.g., "USD" for dollars).
    forms : Optional[List[str]]
        Filing types to include (defaults to ["10-Q", "10-K"]).

    Returns
    -------
    pd.DataFrame
        Columns: [quarter_end, tag] sorted by quarter_end descending.
        One row per period end date (de-duplicated, keeping latest filed).

    Raises
    ------
    KeyError
        If tag or unit not found in CompanyFacts.

    Notes
    -----
    The fp (form period) field distinguishes quarterly (fp=Q1/Q2/Q3/Q4) from
    annual (fp=FY) facts. This function filters to quarterly only.
    """
    if forms is None:
        forms = ["10-Q", "10-K"]

    facts = companyfacts.get("facts", {}).get(taxonomy, {})
    if tag not in facts:
        raise KeyError(f"Tag not found: {taxonomy}:{tag}")

    units = facts[tag].get("units", {})
    if unit not in units:
        raise KeyError(f"Unit '{unit}' not found for {taxonomy}:{tag}. Units: {list(units.keys())}")

    df = pd.DataFrame(units[unit]).copy()
    if "form" in df.columns:
        df = df[df["form"].isin(forms)].copy()
    if "fp" in df.columns:
        df = df[df["fp"].astype(str).str.contains("Q", na=False)].copy()

    df["end"] = pd.to_datetime(df["end"], errors="coerce")
    if "filed" in df.columns:
        df["filed"] = pd.to_datetime(df["filed"], errors="coerce")

    df = df.dropna(subset=["end"])
    sort_cols = ["end"] + (["filed"] if "filed" in df.columns else [])
    df = df.sort_values(sort_cols)
    df = df.drop_duplicates(subset=["end"], keep="last")

    df = df.rename(columns={"end": "quarter_end", "val": tag})
    df = df.sort_values("quarter_end", ascending=False).reset_index(drop=True)
    return df[["quarter_end", tag]]

def extract_fact_all(
    companyfacts: dict,
    tag: str,
    taxonomy: str = "us-gaap",
    unit: str = "USD",
    forms: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Extract all observations for a tag keyed by end date (no fp filtering).

    Unlike extract_quarterly_fact, this returns both quarterly (fp=Q1/Q2/Q3)
    and annual (fp=FY) records. Important for balance sheet tags and cash flow
    items which often appear as YTD in 10-Q.

    Args
    ----
    companyfacts : dict
        Full CompanyFacts JSON.
    tag : str
        XBRL concept name.
    taxonomy : str
        Fact namespace ("us-gaap").
    unit : str
        Unit key ("USD").
    forms : Optional[List[str]]
        Filing types (defaults to ["10-Q", "10-K"]).

    Returns
    -------
    pd.DataFrame
        Columns: [end, tag] sorted by end date ascending.
        One row per period end date (de-duplicated, keeping latest filed).
    """
    if forms is None:
        forms = ["10-Q", "10-K"]

    facts = companyfacts.get("facts", {}).get(taxonomy, {})
    if tag not in facts:
        return pd.DataFrame(columns=["end", tag])

    units = facts[tag].get("units", {})
    if unit not in units:
        return pd.DataFrame(columns=["end", tag])

    df = pd.DataFrame(units[unit]).copy()
    if "form" in df.columns:
        df = df[df["form"].isin(forms)].copy()

    df["end"] = pd.to_datetime(df["end"], errors="coerce")
    if "filed" in df.columns:
        df["filed"] = pd.to_datetime(df["filed"], errors="coerce")
    df = df.dropna(subset=["end"])

    sort_cols = ["end"] + (["filed"] if "filed" in df.columns else [])
    df = df.sort_values(sort_cols).drop_duplicates(subset=["end"], keep="last")

    df = df.rename(columns={"val": tag})
    return df[["end", tag]].sort_values("end")


def coalesce_tags_by_end(
    companyfacts: dict,
    tags: List[str],
    taxonomy: str = "us-gaap",
    unit: str = "USD",
) -> pd.DataFrame:
    """Build a series by end date where value = first non-null across candidate tags.

    Merges multiple candidate tags (e.g., alternate names for the same concept)
    and returns the first non-null value at each end date. Used for concepts
    with multiple possible XBRL representations.

    Args
    ----
    companyfacts : dict
        CompanyFacts JSON.
    tags : List[str]
        Candidate tag names to try (in priority order).
    taxonomy : str
        Fact namespace.
    unit : str
        Unit key.

    Returns
    -------
    pd.DataFrame
        Columns: [end, value] where value is first non-null across tags.

    Example::

        capex_series = coalesce_tags_by_end(facts, [
            "PaymentsToAcquireProductiveAssets",
            "PaymentsToAcquirePropertyPlantAndEquipment",
        ])
    """
    merged = None
    for tag in tags:
        s = extract_fact_all(companyfacts, tag, taxonomy=taxonomy, unit=unit)
        merged = s if merged is None else merged.merge(s, on="end", how="outer")

    if merged is None or merged.empty:
        return pd.DataFrame(columns=["end", "value"])

    merged = merged.sort_values("end")
    tag_cols = [t for t in tags if t in merged.columns]
    merged["value"] = merged[tag_cols].apply(pd.to_numeric, errors="coerce").bfill(axis=1).iloc[:, 0]
    return merged[["end", "value"]]


def ytd_to_quarterly(
    df: pd.DataFrame,
    col: str,
    fiscal_year_end_month: int = 12,
) -> pd.DataFrame:
    """Convert a YTD (year-to-date) series to quarterly increments by differencing.

    HOOD's XBRL filings report many cash flow and expense items on a YTD basis
    in 10-Q: Q1 holds 1-month YTD, Q2 holds 2-month YTD, Q3 holds 3-month YTD.
    This function recovers the true quarterly increment by differencing within
    each fiscal year, avoiding spurious large differences at year boundaries.

    Args
    ----
    df : pd.DataFrame
        Must contain an ``end`` column (period end date) and ``col`` (YTD value).
    col : str
        Column name of the YTD series to convert.
    fiscal_year_end_month : int
        Calendar month (1–12) on which the fiscal year ends. Default is 12
        (December), which is correct for HOOD (Dec 31 year-end). For a June
        fiscal year pass 6; the function shifts dates so that period-end dates
        within the same fiscal year receive the same fiscal-year label.

    Returns
    -------
    pd.DataFrame
        Copy of input df with ``col`` converted to quarterly increments.

    Notes
    -----
    HOOD uses a calendar fiscal year (Dec 31 year-end) so the default of 12 is
    appropriate. This parameter exists to make the function reusable for other
    companies. A mismatch between the actual fiscal year and this parameter
    will cause incorrect YTD differencing (values from two fiscal years will be
    grouped together and produce a spurious large Q1 value).
    """
    if not 1 <= fiscal_year_end_month <= 12:
        raise ValueError(
            f"fiscal_year_end_month must be between 1 and 12, got {fiscal_year_end_month!r}"
        )

    out = df.copy()
    out["end"] = pd.to_datetime(out["end"], errors="coerce")
    out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["end"]).sort_values("end").reset_index(drop=True)

    # Shift period-end dates forward so that the fiscal year boundary aligns
    # with a calendar year boundary. For Dec FY (month=12) shift=0 months
    # (no-op). For Jun FY (month=6) shift=6 months: a period ending 2024-06-30
    # becomes 2024-12-30, staying in calendar year 2024, while 2024-09-30
    # becomes 2025-03-30, landing in calendar year 2025 (= FY2025 which ends
    # 2025-06-30).
    shift_months = 12 - fiscal_year_end_month
    if shift_months == 0:
        out["_fy"] = out["end"].dt.year
    else:
        out["_fy"] = (out["end"] + pd.DateOffset(months=shift_months)).dt.year

    def _diff_group(g: pd.DataFrame) -> pd.DataFrame:
        """Difference within fiscal year; first Q = Q1 YTD (no diff)."""
        g = g.sort_values("end").copy()
        first_val = g[col].iloc[0]  # Q1 YTD == Q1 actual; capture before diff() overwrites it
        g[col] = g[col].diff()
        g.iloc[0, g.columns.get_loc(col)] = first_val
        return g

    out = out.groupby("_fy", group_keys=False).apply(_diff_group, include_groups=False)
    out = out.drop(columns=["_fy"], errors="ignore")  # pandas 2.2+: include_groups=False already removes it
    return out


def _find_revenue_disagg_sheet(xl) -> Optional[str]:
    """Locate the revenue-disaggregation detail sheet inside a HOOD 10-Q XLSX.

    HOOD breaks out three revenue segments in a note schedule:
      - Transaction-based revenues
      - Net interest revenues
      - Other revenues

    Sheet naming changed across filings:
      2022–2023: "REVENUES - REVENUE DISAGGREGATE" or "REVENUES - Revenue Disaggregate"
      2024+:     "REVENUES - Schedule of Revenue"

    Args
    ----
    xl : pd.ExcelFile
        Opened Excel workbook object.

    Returns
    -------
    Optional[str]
        Sheet name if found, else None.
    """
    for name in xl.sheet_names:
        n = name.upper()
        if "REVENUES" in n and ("DISAGGREG" in n or "SCHEDULE OF REV" in n):
            return name
    return None


def parse_revenue_disagg_sheet(df: pd.DataFrame) -> dict:
    """Extract the three top-level revenue segments from a parsed XLSX sheet.

    The HOOD revenue note always has these section headers:
      - "Total net interest revenues"   → Net Interest Revenue total
      - "Transaction-based revenues"    → section header for transaction segment
      - "Other revenues"                → section header for other revenue segment

    The header/total ordering differs by year:
      2024+: header → total immediately after (within 3 rows)
      2023 : sub-items → header → total (same proximity rule applies)

    The current-period 3-month value is always column index 1.

    Args
    ----
    df : pd.DataFrame
        Parsed XLSX sheet (header=None, so column 0 = labels, column 1+ = periods).

    Returns
    -------
    dict
        {"Transaction-based Revenue": val, "Net Interest Revenue": val,
         "Other Revenue": val} in raw dollars (× 1,000,000 from millions).
        Missing values are omitted from the dict.
    """
    COL = 1   # current-period 3-month column (0-indexed; column 1 = 2nd column)

    results: dict = {}
    last_section_header: Optional[str] = None
    last_section_row: int = -99

    for i, row in df.iterrows():
        row_vals = [str(v).strip() for v in row]
        label = row_vals[0] if row_vals else ""
        if label in ("nan", ""):
            continue

        # Parse numeric value from current-period column
        val: Optional[float] = None
        if COL < len(row_vals):
            try:
                val = float(row_vals[COL])
            except (ValueError, TypeError):
                pass

        # Section headers (no numeric value expected)
        if label in ("Transaction-based revenues", "Other revenues",
                     "Net interest revenues"):
            last_section_header = label
            last_section_row    = i
            continue

        # Named totals (direct match)
        if "Total net interest revenues" in label and val is not None:
            results["Net Interest Revenue"] = val * 1_000_000
            continue

        if "Total net revenues" in label and val is not None:
            results["_Total Revenue Check"] = val * 1_000_000
            continue

        # Generic "Total ..." rows: interpret by the section header that preceded them
        # (must be within 3 rows to distinguish section totals from sub-item totals)
        is_total_row = (
            "Total transaction-based revenues" in label
            or "Total other revenues" in label
        )
        if is_total_row and val is not None and last_section_header is not None:
            distance = i - last_section_row
            if distance <= 3:
                if last_section_header == "Transaction-based revenues":
                    results["Transaction-based Revenue"] = val * 1_000_000
                elif last_section_header == "Other revenues":
                    results["Other Revenue"] = val * 1_000_000
                last_section_header = None   # consumed; don't match sub-items

    # Fallback: derive "Other Revenue" as residual if not directly found
    if ("Other Revenue" not in results
            and "Transaction-based Revenue" in results
            and "Net Interest Revenue" in results
            and "_Total Revenue Check" in results):
        residual = (
            results["_Total Revenue Check"]
            - results["Transaction-based Revenue"]
            - results["Net Interest Revenue"]
        )
        if residual >= 0:
            results["Other Revenue"] = residual
        else:
            logger.warning(
                "  [REV SEGMENTS] Other Revenue residual is negative ($%.0f) — "
                "segment totals exceed reported revenue. Discarding residual.",
                residual,
            )

    results.pop("_Total Revenue Check", None)
    return results


def _find_revenue_disagg_htm_url(cik10: str, accn: str) -> Optional[str]:
    """Parse FilingSummary.xml to find the revenue-disaggregation R*.htm page.

    For filings that ship only HTML (no XLSX), fetches FilingSummary.xml and
    searches for a report with "revenue" and "disaggregat" in the name.

    Args
    ----
    cik10 : str
        10-digit CIK.
    accn : str
        Accession number.

    Returns
    -------
    Optional[str]
        Full EDGAR URL to the R*.htm file, or None if not found or on error.
    """
    try:
        import xml.etree.ElementTree as ET
        base = accession_base(cik10, accn)
        xml_text = get_text(f"{base}/FilingSummary.xml")
        root = ET.fromstring(xml_text)
        for report in root.iter():
            if not report.tag.endswith("Report"):
                continue
            name_el = report.find("LongName")
            file_el = report.find("HtmlFileName")
            if name_el is None or file_el is None:
                continue
            name = (name_el.text or "").lower()
            if "revenue" in name and "disaggregat" in name and "details" in name:
                return f"{base}/{file_el.text}"
    except (AttributeError, ValueError) as e:
        logger.debug("  [REV SEGMENTS] Could not parse FilingSummary.xml for %s/%s: %s: %s", cik10, accn, type(e).__name__, e)
    return None


def _parse_revenue_disagg_htm(html: str) -> dict:
    """Parse a REVENUES disaggregation R*.htm XBRL viewer page (HTML-only filings).

    Strategy: strip all HTML tags to plain text, then use regex to extract the
    first numeric column (3-month quarter) for three totals:
      • "Total net interest revenues"                                  → NI
      • "Total transaction-based revenues" (with/without "and other") → Txn
      • Other = Total net revenues − NI − Txn

    Args
    ----
    html : str
        Raw HTML from the R*.htm XBRL page.

    Returns
    -------
    dict
        Partial dict with keys from {"Net Interest Revenue", "Transaction-based Revenue",
        "Other Revenue"}. Missing values are omitted.

    Notes
    -----
    SEC XBRL HTML pages use a fixed layout; this regex approach is brittle and
    may break if SEC redesigns the viewer. The XLSX fallback (parse_revenue_disagg_sheet)
    is preferred.
    """
    import re as _re
    text = _re.sub(r"<[^>]+>", " ", html)
    text = _re.sub(r"&#160;", " ", text)
    text = _re.sub(r"\s+", " ", text)

    results: dict = {}

    def _first_num(pattern: str) -> Optional[float]:
        """Return the first integer following the given regex pattern, in $M.

        Handles parenthesis-notation negatives: e.g. $(123) → -123_000_000.
        """
        m = _re.search(pattern + r"\s*\$?\s*(\()?([\d,]+)\)?", text, _re.IGNORECASE)
        if not m:
            return None
        raw = m.group(2).replace(",", "")
        val = float(raw) * 1_000_000   # $ in millions → raw dollars
        return -val if m.group(1) else val

    ni = _first_num(r"Total net interest revenues")
    total = _first_num(r"Total net revenues")
    txn = _first_num(r"Total transaction-based revenues(?:\s+and\s+other\s+revenues)?")

    if ni is not None:
        results["Net Interest Revenue"] = ni
    if txn is not None:
        results["Transaction-based Revenue"] = txn
    if total is not None and ni is not None and txn is not None:
        residual = total - ni - txn
        if residual >= 0:
            results["Other Revenue"] = residual
        else:
            logger.warning(
                "  [REV SEGMENTS] HTML Other Revenue residual is negative ($%.0f) — "
                "segment totals exceed total revenue. Discarding residual.",
                residual,
            )

    return results


def extract_revenue_segments(
    accn: str,
    out_dir: str = "sec_downloads",
    cik10: Optional[str] = None,
) -> dict:
    """Extract the three revenue segments for a filing accession.

    Primary path: parse the pre-downloaded XLSX (sec_downloads/<accn>.xlsx).
    Fallback path: for HTML-only filings (no XLSX), fetch the revenue
    disaggregation R*.htm page from SEC EDGAR and parse it.

    Args
    ----
    accn : str
        Accession number (e.g., "0001193125-25-000001").
    out_dir : str
        Directory where XLSX is cached (default "sec_downloads").
    cik10 : Optional[str]
        10-digit CIK (required for HTML fallback; can be None to skip fallback).

    Returns
    -------
    dict
        {"Transaction-based Revenue": float|None,
         "Net Interest Revenue":      float|None,
         "Other Revenue":             float|None}
        Keys with None values are omitted for brevity.
    """
    empty = {
        "Transaction-based Revenue": None,
        "Net Interest Revenue":      None,
        "Other Revenue":             None,
    }

    # --- Primary: XLSX path ---
    xlsx_path = os.path.join(out_dir, f"{accn}.xlsx")
    if os.path.exists(xlsx_path):
        try:
            import openpyxl as _openpyxl  # noqa: F401 (ensure it's available)
            with pd.ExcelFile(xlsx_path, engine="openpyxl") as xl:
                sheet_name = _find_revenue_disagg_sheet(xl)
                if sheet_name:
                    df = xl.parse(sheet_name, header=None)
                    segs = parse_revenue_disagg_sheet(df)
                    return {k: segs.get(k) for k in empty}
        except (OSError, ValueError, KeyError) as e:
            logger.warning("  [REV SEGMENTS] Could not parse %s: %s: %s — falling back to HTML",
                           xlsx_path, type(e).__name__, e)

    # --- Fallback: R*.htm page from FilingSummary.xml ---
    if cik10 is None:
        return empty
    try:
        htm_url = _find_revenue_disagg_htm_url(cik10, accn)
        if not htm_url:
            return empty
        html = get_text(htm_url)
        segs = _parse_revenue_disagg_htm(html)
        result = {k: segs.get(k) for k in empty}
        if any(v is not None for v in result.values()):
            logger.debug("  [REV SEGMENTS] Parsed from HTML R*.htm fallback (%s)", accn)
        return result
    except requests.exceptions.RequestException as e:
        logger.debug("  [REV SEGMENTS] HTML fallback failed for %s: %s: %s", accn, type(e).__name__, e)
        return empty


def wide_from_series(df: pd.DataFrame, line_item: str, date_col: str, value_col: str, quarters: int) -> pd.DataFrame:
    """Convert a long (date, value) series to the wide row format used by this script.

    Args
    ----
    df : pd.DataFrame
        Long-format DataFrame with columns including date_col and value_col.
    line_item : str
        Name to put in the "Line Item" column of the output.
    date_col : str
        Column name with period end dates.
    value_col : str
        Column name with numeric values.
    quarters : int
        Maximum number of quarters to include (most recent first).

    Returns
    -------
    pd.DataFrame
        Wide-format DataFrame with one row (the line item), columns = ISO date strings.

    Example::

        df_long = pd.DataFrame({
            "end": ["2024-12-31", "2024-09-30"],
            "Revenue": [1000, 800]
        })
        wide_from_series(df_long, "Revenue", "end", "Revenue", 2)
        # → 1 row, columns = ["Line Item", "2024-12-31", "2024-09-30"]
    """
    s = df[[date_col, value_col]].dropna(subset=[date_col]).copy()
    s[date_col] = pd.to_datetime(s[date_col], errors="coerce")
    s = s.dropna(subset=[date_col]).sort_values(date_col, ascending=False).head(quarters)
    wide = s.set_index(date_col).T
    wide.columns = [d.date().isoformat() for d in wide.columns]
    wide.insert(0, "Line Item", [line_item])
    return wide.reset_index(drop=True)


def sum_quarterly_facts(
    companyfacts: dict,
    tags: List[str],
    taxonomy: str = "us-gaap",
    unit: str = "USD",
    quarters: int = 10,
) -> pd.DataFrame:
    """Sum quarterly facts across multiple candidate tags.

    Merges data from multiple tags at each quarter end, then sums their values.
    Used for income statement items where a concept may have multiple XBRL tags.

    Args
    ----
    companyfacts : dict
        CompanyFacts JSON.
    tags : List[str]
        XBRL concept names to sum (e.g., ["Revenues", "RevenueFromContractWithCustomer"]).
    taxonomy : str
        Fact namespace ("us-gaap").
    unit : str
        Unit key ("USD").
    quarters : int
        Number of quarters to include.

    Returns
    -------
    pd.DataFrame
        Columns: [quarter_end, SUM] where SUM is the sum of all tags at that date.
    """
    merged: pd.DataFrame | None = None
    for tag in tags:
        df = extract_quarterly_fact(companyfacts, tag, taxonomy=taxonomy, unit=unit).head(quarters)
        merged = df if merged is None else merged.merge(df, on="quarter_end", how="outer")
    if merged is None:
        return pd.DataFrame(columns=["quarter_end", "SUM"])
    merged = merged.sort_values("quarter_end", ascending=False).head(quarters)
    vals = merged[tags].apply(pd.to_numeric, errors="coerce")
    merged["SUM"] = vals.sum(axis=1, min_count=1)
    return merged[["quarter_end", "SUM"]]


def sum_balance_sheet_facts(
    companyfacts: dict,
    tags: List[str],
    taxonomy: str = "us-gaap",
    unit: str = "USD",
    quarters: int = 10,
) -> pd.DataFrame:
    """Sum point-in-time (balance sheet) facts across candidate tags.

    Uses extract_fact_all (no fp filter) so BS tags with fp=FY or fp=Q* are both captured.
    Unlike sum_quarterly_facts, this does NOT filter on fp, which is required for
    broker-dealer liability tags that may be filed under various fp labels.

    Args
    ----
    companyfacts : dict
        CompanyFacts JSON.
    tags : List[str]
        XBRL balance sheet concept names.
    taxonomy : str
        Fact namespace ("us-gaap").
    unit : str
        Unit key ("USD").
    quarters : int
        Number of periods to include.

    Returns
    -------
    pd.DataFrame
        Columns: [quarter_end, SUM] (note: column name is "quarter_end" for consistency).
    """
    merged = None
    found_tags: List[str] = []
    for tag in tags:
        df = extract_fact_all(companyfacts, tag, taxonomy=taxonomy, unit=unit)
        if df.empty:
            continue
        found_tags.append(tag)
        merged = df if merged is None else merged.merge(df, on="end", how="outer")

    if merged is None or merged.empty:
        return pd.DataFrame(columns=["quarter_end", "SUM"])

    merged = merged.sort_values("end", ascending=False).head(quarters)
    tag_cols = [t for t in found_tags if t in merged.columns]
    if not tag_cols:
        return pd.DataFrame(columns=["quarter_end", "SUM"])

    vals = merged[tag_cols].apply(pd.to_numeric, errors="coerce")
    merged["SUM"] = vals.sum(axis=1, min_count=1)
    return merged[["end", "SUM"]].rename(columns={"end": "quarter_end"})


def derive_q4_from_annual(
    companyfacts: dict,
    tag: str,
    taxonomy: str = "us-gaap",
    unit: str = "USD",
) -> pd.DataFrame:
    """For true-quarterly XBRL tags, derive Q4 = FY_annual (10-K) − (Q1 + Q2 + Q3).

    XBRL filers tag each income-statement period as its actual 3-month value under
    fp=Q1/Q2/Q3, and report the full fiscal year under fp=FY in the 10-K. There is
    no separate fp=Q4 record. Subtracting the three interim quarters from the annual
    produces the Q4 incremental period.

    This is critical for income statement items (Revenue, OpEx, Net Income) because
    filings often do not contain the most recent quarter (Q4) until the 10-K is filed.

    Args
    ----
    companyfacts : dict
        CompanyFacts JSON.
    tag : str
        XBRL concept name (e.g., "Revenues", "NetIncomeLoss").
    taxonomy : str
        Fact namespace ("us-gaap").
    unit : str
        Unit key ("USD").

    Returns
    -------
    pd.DataFrame
        Columns: [end, tag] including Q4 entries at Dec 31 dates.
        Sorted by end date.

    Notes
    -----
    If the fp (fiscal period) column is missing or the tag is not found,
    falls back to extract_fact_all (no Q4 derivation).
    """
    facts_data = companyfacts.get("facts", {}).get(taxonomy, {})
    if tag not in facts_data:
        return pd.DataFrame(columns=["end", tag])

    units_data = facts_data[tag].get("units", {})
    if unit not in units_data:
        return pd.DataFrame(columns=["end", tag])

    raw = pd.DataFrame(units_data[unit]).copy()
    if "form" in raw.columns:
        raw = raw[raw["form"].isin(["10-Q", "10-K"])].copy()

    raw["end"] = pd.to_datetime(raw["end"], errors="coerce")
    if "filed" in raw.columns:
        raw["filed"] = pd.to_datetime(raw["filed"], errors="coerce")
    raw = raw.dropna(subset=["end"])

    if "fp" not in raw.columns:
        # Cannot distinguish quarterly vs annual; fall back to simple all-record extraction
        return extract_fact_all(companyfacts, tag, taxonomy=taxonomy, unit=unit)

    sort_cols = ["end"] + (["filed"] if "filed" in raw.columns else [])
    raw = raw.sort_values(sort_cols).drop_duplicates(subset=["end", "fp"], keep="last")
    raw["val"] = pd.to_numeric(raw["val"], errors="coerce")

    # True 3-month quarterly records (fp in {Q1, Q2, Q3})
    q_mask = raw["fp"].astype(str).str.match(r"^Q\d$", na=False)
    q_recs = raw[q_mask].drop_duplicates(subset=["end"], keep="last").copy()

    # Full-year records (fp == FY, sourced from 10-K)
    fy_recs = raw[raw["fp"].astype(str) == "FY"].drop_duplicates(subset=["end"], keep="last").copy()

    # Derive Q4 for each fiscal year where all three interim quarters are available
    q4_rows: List[dict] = []
    for _, fy_row in fy_recs.iterrows():
        fy_end = fy_row["end"]
        fy_val = fy_row["val"]
        year   = fy_end.year
        q_year = q_recs[q_recs["end"].dt.year == year]
        if len(q_year) == 3:
            q4_val = fy_val - q_year["val"].sum()
            q4_rows.append({"end": pd.Timestamp(year=year, month=12, day=31), tag: q4_val})

    # Combine true-quarterly records with derived Q4 values
    q_long = q_recs[["end", "val"]].rename(columns={"val": tag})
    if q4_rows:
        q4_df = pd.DataFrame(q4_rows)
        q_long = pd.concat([q_long, q4_df], ignore_index=True)

    return (
        q_long
        .drop_duplicates(subset=["end"], keep="last")
        .sort_values("end")
        .reset_index(drop=True)
    )


@dataclass
class LineSpec:
    """Specification for extracting a single line item from CompanyFacts.

    Attributes
    ----------
    line_name : str
        Output line name (e.g., "Total Revenue").
    preferred_tags : List[str]
        Candidate XBRL tag names, in priority order.
    regex_fallback : Optional[str]
        Regex pattern to match tags if none of the preferred tags exist.
    taxonomy : str
        XBRL namespace ("us-gaap" or "ifrs-full").
    unit : str
        XBRL unit ("USD", "EUR", etc.).
    """
    line_name: str
    preferred_tags: List[str]
    regex_fallback: Optional[str] = None
    taxonomy: str = "us-gaap"
    unit: str = "USD"


def _tag_exists(companyfacts: dict, tag: str, taxonomy: str) -> bool:
    """Check if a tag exists in CompanyFacts."""
    return tag in companyfacts.get("facts", {}).get(taxonomy, {})


def _regex_find_tag(companyfacts: dict, pattern: str, taxonomy: str) -> Optional[str]:
    """Find the first tag matching a regex pattern (case-insensitive)."""
    rx = re.compile(pattern, re.IGNORECASE)
    facts = companyfacts.get("facts", {}).get(taxonomy, {})
    for t in sorted(facts.keys()):
        if rx.search(t):
            return t
    return None


def resolve_tag(companyfacts: dict, spec: LineSpec) -> Tuple[Optional[str], str]:
    """Resolve a LineSpec to an actual XBRL tag, or None if not found.

    Tries preferred_tags first, then falls back to regex_fallback.

    Returns
    -------
    Tuple[Optional[str], str]
        (resolved_tag_name, reason_string) where reason is one of:
        - "preferred" (found in preferred_tags)
        - "regex:pattern" (found via regex_fallback)
        - "missing" (not found)
    """
    for t in spec.preferred_tags:
        if _tag_exists(companyfacts, t, spec.taxonomy):
            return t, "preferred"
    if spec.regex_fallback:
        found = _regex_find_tag(companyfacts, spec.regex_fallback, spec.taxonomy)
        if found:
            return found, f"regex:{spec.regex_fallback}"
    return None, "missing"


def build_companyfacts_statement(
    companyfacts: dict,
    specs: List[LineSpec],
    quarters: int = 10,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build a wide-format financial statement from CompanyFacts using LineSpec list.

    For each LineSpec, resolves the XBRL tag and extracts quarterly time series.
    Returns both the wide-format statement (one row per line item) and an audit
    report showing which tags were resolved and any extraction errors.

    Args
    ----
    companyfacts : dict
        CompanyFacts JSON.
    specs : List[LineSpec]
        List of line item specifications.
    quarters : int
        Number of quarters to include (most recent first).

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        (wide_statement, tag_report) where:
        - wide_statement: columns [Line Item, date1, date2, ...] (ISO format)
        - tag_report: audit trail with columns [Statement Line, Resolved Tag, Reason, ...]
    """
    report_rows = []
    merged = None

    for spec in specs:
        tag, reason = resolve_tag(companyfacts, spec)
        report_rows.append(
            {
                "Statement Line": spec.line_name,
                "Resolved Tag": tag or "",
                "Reason": reason,
                "Taxonomy": spec.taxonomy,
                "Unit": spec.unit,
            }
        )

        if not tag:
            s = pd.DataFrame({"quarter_end": [], spec.line_name: []})
        else:
            try:
                s_raw = extract_quarterly_fact(companyfacts, tag, taxonomy=spec.taxonomy, unit=spec.unit).head(quarters)
                s = s_raw.rename(columns={tag: spec.line_name})
            except (KeyError, IndexError) as e:
                report_rows[-1]["Reason"] = f"{reason}|extract_fail:{type(e).__name__}"
                s = pd.DataFrame({"quarter_end": [], spec.line_name: []})

        merged = s if merged is None else merged.merge(s, on="quarter_end", how="outer")

    if merged is None or merged.empty:
        wide = pd.DataFrame()
    else:
        merged = merged.sort_values("quarter_end", ascending=False).head(quarters)
        wide = merged.set_index("quarter_end").T
        wide.columns = [d.date().isoformat() for d in wide.columns]
        wide.insert(0, "Line Item", wide.index)
        wide = wide.reset_index(drop=True)

    return wide, pd.DataFrame(report_rows)


def add_fcf_row(cf_tbl: pd.DataFrame, cfo_line: str, capex_line: str) -> pd.DataFrame:
    """Add a Free Cash Flow row to a wide-format cash flow statement.

    Computes FCF = CFO − Capex and appends as a new row.

    Args
    ----
    cf_tbl : pd.DataFrame
        Wide-format cash flow statement.
    cfo_line : str
        Line Item name for CFO (e.g., "Cash From Operations (CFO)").
    capex_line : str
        Line Item name for Capex (e.g., "Capex (Productive Assets)").

    Returns
    -------
    pd.DataFrame
        Copy of cf_tbl with FCF row appended (or original if missing required rows).
    """
    if cf_tbl.empty:
        return cf_tbl
    cf = cf_tbl.set_index("Line Item")
    cols = list(cf.columns)
    if cfo_line not in cf.index or capex_line not in cf.index:
        return cf_tbl
    cfo = pd.to_numeric(cf.loc[cfo_line], errors="coerce")
    capex = pd.to_numeric(cf.loc[capex_line], errors="coerce")
    fcf = cfo - capex
    row = pd.DataFrame([["Free Cash Flow"] + [fcf.get(c, None) for c in cols]], columns=["Line Item"] + cols)
    return pd.concat([cf_tbl, row], ignore_index=True)


# ---------------------------------------------------------------------------
# Main Extraction Pipelines
# ---------------------------------------------------------------------------

def run_statement_pipeline(ticker: str, cik10: str, quarters: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract financial statements from 10-Q XLSX and FilingSummary HTML.

    This pipeline is statement-focused: it parses the actual 10-Q statements
    line-by-line, extracting user-specified line items via regex matching.
    Output is one row per accession (filing), not one row per line item.

    Args
    ----
    ticker : str
        Stock ticker (used only for logging).
    cik10 : str
        10-digit CIK.
    quarters : int
        Number of recent 10-Q filings to process.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        (df_is, df_bs, df_cf) where each has columns:
        [report_date, accession, line_item1, line_item2, ...]
    """
    filings = get_last_10q_accessions(cik10, quarters)

    out_dir = str(REPO_ROOT / "sec_downloads")
    os.makedirs(out_dir, exist_ok=True)

    # Line item extraction patterns. Regex patterns are matched against raw statement
    # line items (case-insensitive). The first pattern to match provides the value.
    wanted_is = {
        # HOOD uses labels like "Transaction-based revenues" and "Net interest revenues"
        # Keep these patterns literal to avoid false matches.
        "Transaction-based revenue": [r"\btransaction[- ]based revenues?\b"],
        "Net interest revenue": [r"\bnet interest revenues?\b"],
        # Never match generic "other" (it will pull other income/expense, OCI, etc.)
        "Other revenue": [r"\bother revenues?\b"],
        "Total revenue": [r"\btotal (?:net )?revenues?\b", r"\btotal net revenues?\b", r"\btotal revenues?\b"],
        "Operating expenses": [r"\btotal operating expenses\b", r"\boperating expenses\b"],
        # Often not a standalone statement line; may remain blank (that is OK).
        "Stock-based compensation": [r"\bstock[- ]based compensation\b", r"\bshare[- ]based compensation\b"],
        "Net income": [r"\bnet income\b", r"\bnet loss\b"],
    }

    wanted_bs = {
        "Cash & equivalents": [r"cash and cash equivalents"],
        "Restricted cash": [r"restricted cash"],
        "Assets under custody": [r"assets under custody", r"\bauc\b", r"custody"],
        "Receivables": [r"receivables", r"receivable"],
        "Debt": [r"long[- ]term debt", r"total debt", r"borrowings", r"notes payable", r"convertible"],
        "Payables": [r"accounts payable", r"payables", r"accrued"],
        "Equity": [r"total.*equity", r"stockholders.? equity", r"shareholders.? equity"],
    }

    wanted_cf = {
        "Net income": [r"net income", r"net loss"],
        "Stock-based compensation": [r"stock[- ]based compensation", r"share[- ]based compensation"],
        "Change in working capital": [r"changes? in operating assets and liabilities", r"change in working capital"],
        "Capex": [r"purchase of property", r"payments to acquire property", r"capital expenditures"],
        "CFO": [r"net cash.*operating activities"],
    }

    rows_is, rows_bs, rows_cf = [], [], []

    for accn, report_date in filings:
        logger.info("\n[STATEMENTS] Processing %s (%s)", accn, report_date)

        xlsx_url = find_best_xlsx(cik10, accn)
        xlsx_path = None

        if xlsx_url:
            xlsx_path = os.path.join(out_dir, f"{accn}.xlsx")
            if not os.path.exists(xlsx_path):
                try:
                    download_file(xlsx_url, xlsx_path)
                    logger.debug("[STATEMENTS] Downloaded XLSX")
                except (requests.exceptions.RequestException, OSError) as e:
                    logger.warning("[STATEMENTS] XLSX download failed: %s: %s", type(e).__name__, e)
                    xlsx_path = None

        stmt_is = stmt_bs = stmt_cf = None

        if xlsx_path and os.path.exists(xlsx_path):
            try:
                stmt_is = extract_statement_from_xlsx(xlsx_path, "is")
                stmt_bs = extract_statement_from_xlsx(xlsx_path, "bs")
                stmt_cf = extract_statement_from_xlsx(xlsx_path, "cf")
                logger.debug("[STATEMENTS] Parsed from XLSX")
            except (OSError, KeyError, IndexError, RuntimeError) as e:
                logger.warning("[STATEMENTS] XLSX parse failed: %s: %s", type(e).__name__, e)
                stmt_is = stmt_bs = stmt_cf = None

        # Fallback to FilingSummary.xml HTML statement pages
        if stmt_is is None or stmt_bs is None or stmt_cf is None:
            urls = filing_summary_statement_urls(cik10, accn)
            if len(urls) == 3:
                try:
                    stmt_is = parse_statement_html(urls["is"])
                    stmt_bs = parse_statement_html(urls["bs"])
                    stmt_cf = parse_statement_html(urls["cf"])
                    logger.debug("[STATEMENTS] Parsed from FilingSummary HTML")
                except (AttributeError, ValueError) as e:
                    logger.warning("[STATEMENTS] HTML parse failed: %s: %s", type(e).__name__, e)
            else:
                logger.warning("[STATEMENTS] FilingSummary statement URLs not found")

        if stmt_is is None or stmt_bs is None or stmt_cf is None:
            logger.warning("[STATEMENTS] Skipping accession: no parsable statements")
            continue

        is_vals = pick_lines(stmt_is, wanted_is)
        bs_vals = pick_lines(stmt_bs, wanted_bs)
        cf_vals = pick_lines(stmt_cf, wanted_cf)

        cfo = cf_vals.get("CFO")
        capex = cf_vals.get("Capex")
        fcf = (cfo - capex) if (cfo is not None and capex is not None) else None

        rows_is.append({"report_date": report_date, "accession": accn, **is_vals})
        rows_bs.append({"report_date": report_date, "accession": accn, **bs_vals})
        rows_cf.append({"report_date": report_date, "accession": accn, **cf_vals, "Free cash flow": fcf})

    df_is = pd.DataFrame(rows_is).sort_values("report_date", ascending=False)
    df_bs = pd.DataFrame(rows_bs).sort_values("report_date", ascending=False)
    df_cf = pd.DataFrame(rows_cf).sort_values("report_date", ascending=False)
    return df_is, df_bs, df_cf


def run_companyfacts_pipeline(ticker: str, cik10: str, quarters: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract financial statements from SEC CompanyFacts (XBRL).

    This pipeline is GAAP-focused: it extracts XBRL concepts (accounting facts)
    and produces wide-format statements (one row per line item, columns per quarter).
    It handles YTD→quarterly conversion, derives Q4 from annual, and pulls revenue
    segments from the 10-Q XLSX note schedules.

    Args
    ----
    ticker : str
        Stock ticker (used for logging and output naming).
    cik10 : str
        10-digit CIK.
    quarters : int
        Number of quarters to include in output.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
        (is_tbl, bs_tbl, cf_tbl, tag_report) where:
        - is_tbl, bs_tbl, cf_tbl: wide-format statements (one row per line item)
        - tag_report: audit trail showing XBRL tag resolution for each line item
    """
    logger.info("\n[COMPANYFACTS] Downloading companyfacts")
    facts = get_companyfacts(cik10)

    out_dir = str(REPO_ROOT / "sec_downloads")

    # Use the same 10-Q report dates as the statement pipeline to avoid mixing in 10-K year-end points.
    filings_10q = get_last_10q_accessions(cik10, quarters)
    report_dates_10q = {rdate for _, rdate in filings_10q if rdate}

    logger.debug("report_dates_10q: %s", sorted(report_dates_10q))
    drop_year_end_if_no_report_dates = (len(report_dates_10q) == 0)

    # --------------- Pre-compute corrected SBC ---------------
    # ShareBasedCompensation is YTD-cumulative in HOOD's XBRL: the Q3 fp tag holds the
    # 9-month total, not the 3-month amount. Route through extract_fact_all (which
    # includes 10-K records) + ytd_to_quarterly to recover true quarterly values and
    # derive Q4 as FY_total − Q3_YTD.
    _sbc_long = extract_fact_all(facts, "ShareBasedCompensation", taxonomy="us-gaap", unit="USD")
    _sbc_long = _sbc_long.rename(columns={"ShareBasedCompensation": "_SBC_YTD"})
    sbc_q = ytd_to_quarterly(_sbc_long, "_SBC_YTD").rename(
        columns={"_SBC_YTD": "Stock-Based Compensation"}
    )

    # --------------- IS: Extract each item with Q4 support ---------------
    # Revenue, OpEx, and Net Income are true-quarterly in HOOD's XBRL (fp=Q1/Q2/Q3
    # carries the 3-month figure). Q4 is derived as FY_annual (10-K) − (Q1+Q2+Q3).
    def _resolve_is_tag(candidates: List[str]) -> Optional[str]:
        return next((t for t in candidates if _tag_exists(facts, t, "us-gaap")), None)

    def _derive_or_empty(tag: Optional[str], label: str) -> pd.DataFrame:
        if not tag:
            return pd.DataFrame(columns=["end", label])
        return derive_q4_from_annual(
            facts, tag, taxonomy="us-gaap", unit="USD"
        ).rename(columns={tag: label})

    rev_tag  = _resolve_is_tag(["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])
    opex_tag = _resolve_is_tag(["OperatingExpenses"])
    ni_tag   = _resolve_is_tag(["NetIncomeLoss"])

    rev_long  = _derive_or_empty(rev_tag,  "Total Revenue")
    opex_long = _derive_or_empty(opex_tag, "Operating Expenses")
    ni_long   = _derive_or_empty(ni_tag,   "Net Income")

    _is_parts = [df for df in [rev_long, opex_long, sbc_q, ni_long] if not df.empty]
    _is_merged = _is_parts[0].copy() if _is_parts else pd.DataFrame(columns=["end"])
    for _part in _is_parts[1:]:
        _is_merged = _is_merged.merge(_part, on="end", how="outer")
    _is_merged = _is_merged.sort_values("end", ascending=False)

    if report_dates_10q:
        _years    = {pd.Timestamp(d).year for d in report_dates_10q}
        _q4_dates = {f"{yr}-12-31" for yr in _years}
        _keep     = report_dates_10q | _q4_dates
        _is_merged = _is_merged[
            _is_merged["end"].dt.strftime("%Y-%m-%d").isin(_keep)
        ].copy()

    _is_merged = _is_merged.head(quarters + 4)   # +4 to accommodate added Q4 rows

    # --------------- D&A Calibration Log ---------------
    # D&A is not extracted as a separate line item (HOOD's XBRL reporting is
    # inconsistent for this tag). Search for known tags; if found, compute the
    # historical ratio so users can calibrate the da_pct Assumption.
    _da_tags = search_tags(facts, r"DepreciationAndAmortization")
    if _da_tags and not rev_long.empty:
        _da_tag = _da_tags[0]
        try:
            _da_long = extract_fact_all(facts, _da_tag, taxonomy="us-gaap", unit="USD")
            if not _da_long.empty:
                _rev_renamed = rev_long.rename(columns={"Total Revenue": "_rev"})
                _da_merged = _da_long.merge(_rev_renamed, on="end", how="inner")
                _da_merged[_da_tag] = pd.to_numeric(_da_merged[_da_tag], errors="coerce")
                _da_merged["_rev"]  = pd.to_numeric(_da_merged["_rev"],  errors="coerce")
                _ratios = (_da_merged[_da_tag] / _da_merged["_rev"]).dropna()
                if not _ratios.empty:
                    logger.info(
                        "[D&A CALIBRATION] Tag '%s' found — historical avg D&A / Revenue = %.1f%%. "
                        "Compare against the da_pct assumption (default 3%%).",
                        _da_tag, _ratios.mean() * 100,
                    )
        except Exception as _e:
            logger.debug("  [D&A] Could not extract D&A for calibration: %s", _e)
    else:
        logger.warning(
            "[D&A CALIBRATION] No DepreciationAndAmortization XBRL tag found. "
            "Using da_pct assumption (default 3%%). Verify against 10-Q disclosures."
        )

    if not _is_merged.empty:
        _is_wide = _is_merged.set_index("end").T
        _is_wide.columns = [d.date().isoformat() for d in _is_wide.columns]
        _is_wide.insert(0, "Line Item", _is_wide.index)
        is_tbl = _is_wide.reset_index(drop=True)
    else:
        is_tbl = pd.DataFrame()

    is_rep = pd.DataFrame([
        {"Statement Line": "Total Revenue",           "Resolved Tag": rev_tag  or "",
         "Reason": "preferred+Q4_derived",           "Taxonomy": "us-gaap", "Unit": "USD"},
        {"Statement Line": "Operating Expenses",      "Resolved Tag": opex_tag or "",
         "Reason": "preferred+Q4_derived",           "Taxonomy": "us-gaap", "Unit": "USD"},
        {"Statement Line": "Stock-Based Compensation","Resolved Tag": "ShareBasedCompensation",
         "Reason": "ytd_to_quarterly+Q4_derived",    "Taxonomy": "us-gaap", "Unit": "USD"},
        {"Statement Line": "Net Income",              "Resolved Tag": ni_tag   or "",
         "Reason": "preferred+Q4_derived",           "Taxonomy": "us-gaap", "Unit": "USD"},
    ])

    # --------------- Revenue Segments from XLSX Note ---------------
    # XBRL rolls all revenue into a single Revenues tag; the segment breakdown
    # (Transaction-based, Net Interest, Other) only exists in the note schedule.
    # We parse the pre-downloaded 10-Q XLSXs and stitch the values into is_tbl.
    logger.info("[COMPANYFACTS] Extracting revenue segments from XLSX note schedules…")
    seg_by_date: dict[str, dict] = {}   # "2025-03-31" → {seg: raw_dollar_value}
    for accn, report_date in filings_10q:
        segs = extract_revenue_segments(accn, cik10=cik10, out_dir=out_dir)
        if any(v is not None for v in segs.values()):
            seg_by_date[report_date] = segs
            logger.debug("  %s: Txn=%s, NI=%s, Other=%s",
                         report_date,
                         segs.get('Transaction-based Revenue'),
                         segs.get('Net Interest Revenue'),
                         segs.get('Other Revenue'))
        else:
            logger.debug("  %s: no segment data found in XLSX", report_date)

    if seg_by_date and not is_tbl.empty:
        is_date_cols = [c for c in is_tbl.columns if c != "Line Item"]
        for seg_label in ("Transaction-based Revenue", "Net Interest Revenue", "Other Revenue"):
            row_vals: dict[str, object] = {"Line Item": seg_label}
            for dc in is_date_cols:
                row_vals[dc] = seg_by_date.get(dc, {}).get(seg_label)
            seg_row = pd.DataFrame([row_vals])
            is_tbl = pd.concat([seg_row, is_tbl], ignore_index=True)

    # --------------- Balance Sheet ---------------
    bs_specs = [
        LineSpec("Cash & Equivalents", ["CashAndCashEquivalentsAtCarryingValue"], regex_fallback=r"CashAndCashEquivalents"),
        LineSpec("Restricted Cash", ["RestrictedCashCurrent", "RestrictedCash", "RestrictedCashNoncurrent"], regex_fallback=r"RestrictedCash"),
        LineSpec("Receivables", ["ReceivablesFromBrokersDealersAndClearingOrganizations", "ReceivablesCurrent", "ReceivablesNetCurrent"], regex_fallback=r"Receiv"),
        LineSpec("Payables", ["AccountsPayableAndAccruedLiabilitiesCurrent", "AccountsPayableCurrent"], regex_fallback=r"Payable|Accrued"),
        LineSpec("Total Equity", ["StockholdersEquity"], regex_fallback=r"StockholdersEquity"),
    ]

    bs_tbl, bs_rep = build_companyfacts_statement(facts, bs_specs, quarters=quarters)

    # --------------- Cash Flow (Quarterly) ---------------
    # Net income + SBC can be taken directly (often quarter-based).
    # CFO + Capex are commonly YTD in 10-Q, so convert to quarterly increments.

    ni_raw = extract_quarterly_fact(facts, "NetIncomeLoss", taxonomy="us-gaap", unit="USD").head(quarters)
    ni_series = ni_raw.rename(columns={"quarter_end": "end", "NetIncomeLoss": "Net Income"})

    # SBC already computed above as sbc_q (YTD-corrected, Q4 derived from 10-K).
    sbc_series = sbc_q  # columns: [end, Stock-Based Compensation]

    # CFO (YTD -> Q)
    cfo_long = coalesce_tags_by_end(
        facts,
        tags=["NetCashProvidedByUsedInOperatingActivities"],
        taxonomy="us-gaap",
        unit="USD",
    ).rename(columns={"value": "CFO_YTD"})
    cfo_long = cfo_long.sort_values("end").reset_index(drop=True)
    cfo_q = ytd_to_quarterly(cfo_long, "CFO_YTD").rename(columns={"CFO_YTD": "Cash From Operations (CFO)"})

    # Capex (coalesce candidates) (YTD -> Q)
    capex_tags = [
        "PaymentsToAcquireProductiveAssets",
        "PaymentsToAcquireOtherProductiveAssets",
        "PaymentsToAcquirePropertyPlantAndEquipment",
    ]
    capex_long = coalesce_tags_by_end(
        facts,
        tags=capex_tags,
        taxonomy="us-gaap",
        unit="USD",
    ).rename(columns={"value": "Capex_YTD"})
    capex_long = capex_long.sort_values("end").reset_index(drop=True)
    capex_q = ytd_to_quarterly(capex_long, "Capex_YTD").rename(columns={"Capex_YTD": "Capex (Productive Assets)"})

    capex_q["Capex (Productive Assets)"] = pd.to_numeric(capex_q["Capex (Productive Assets)"], errors="coerce")
    # Normalize to positive cash outflow (Capex is an outflow; XBRL sign conventions vary)
    capex_q["Capex (Productive Assets)"] = capex_q["Capex (Productive Assets)"].abs()

    # Merge and compute FCF
    cf_long = (
        ni_series.merge(sbc_series, on="end", how="outer")
        .merge(cfo_q, on="end", how="outer")
        .merge(capex_q, on="end", how="outer")
        .sort_values("end")
    )

    # Keep 10-Q report dates AND Dec 31 year-end dates (Q4 derived from 10-K annual data).
    if report_dates_10q:
        _cf_years   = {pd.Timestamp(d).year for d in report_dates_10q}
        _cf_q4dates = {f"{yr}-12-31" for yr in _cf_years}
        _cf_keep    = report_dates_10q | _cf_q4dates
        cf_long = cf_long[cf_long["end"].dt.strftime("%Y-%m-%d").isin(_cf_keep)].copy()
        cf_long = cf_long.sort_values("end")
    elif drop_year_end_if_no_report_dates:
        # If submissions reportDate is missing, drop 12/31 year-end points (usually 10-K)
        cf_long = cf_long[cf_long["end"].dt.month.isin([3, 6, 9])].copy()
        cf_long = cf_long.sort_values("end")

    # Compute Free Cash Flow (CFO - Capex) after filtering to the desired quarter ends
    cf_long["Free Cash Flow"] = (
        pd.to_numeric(cf_long["Cash From Operations (CFO)"], errors="coerce")
        - pd.to_numeric(cf_long["Capex (Productive Assets)"], errors="coerce")
    )

    # Build wide CF table rows
    cf_rows = []
    cf_rows.append(wide_from_series(cf_long, "Net Income", "end", "Net Income", quarters))
    cf_rows.append(wide_from_series(cf_long, "Stock-Based Compensation", "end", "Stock-Based Compensation", quarters))
    cf_rows.append(
        wide_from_series(cf_long, "Cash From Operations (CFO)", "end", "Cash From Operations (CFO)", quarters))
    cf_rows.append(wide_from_series(cf_long, "Capex (Productive Assets)", "end", "Capex (Productive Assets)", quarters))
    cf_rows.append(wide_from_series(cf_long, "Free Cash Flow", "end", "Free Cash Flow", quarters))
    cf_tbl = pd.concat(cf_rows, ignore_index=True)

    # Cash flow tag report (manual because CFO/Capex are derived)
    cf_rep = pd.DataFrame(
        [
            {"Statement Line": "Net Income", "Resolved Tag": "NetIncomeLoss", "Reason": "preferred",
             "Taxonomy": "us-gaap", "Unit": "USD"},
            {"Statement Line": "Stock-Based Compensation", "Resolved Tag": "ShareBasedCompensation",
             "Reason": "preferred", "Taxonomy": "us-gaap", "Unit": "USD"},
            {"Statement Line": "Cash From Operations (CFO)",
             "Resolved Tag": "NetCashProvidedByUsedInOperatingActivities", "Reason": "preferred|ytd_to_quarterly",
             "Taxonomy": "us-gaap", "Unit": "USD"},
            {"Statement Line": "Capex (Productive Assets)", "Resolved Tag": " / ".join(capex_tags),
             "Reason": "preferred_coalesce|ytd_to_quarterly", "Taxonomy": "us-gaap", "Unit": "USD"},
            {"Statement Line": "Free Cash Flow", "Resolved Tag": "CFO - Capex", "Reason": "derived",
             "Taxonomy": "us-gaap", "Unit": "USD"},
        ]
    )


    # --------------- Debt Proxy (Balance Sheet) ---------------
    # Debt proxy: use balance-sheet-style extraction (no fp filter) with an expanded
    # HOOD-specific tag list. The prior implementation used sum_quarterly_facts which
    # filters fp to "Q*" records, silently dropping BS tags filed under fp=FY or
    # non-standard labels. HOOD is a broker-dealer; standard debt tags may not capture
    # securities-lending or credit-facility liabilities.
    _debt_tags = [
        "LongTermDebt",
        "LongTermDebtNoncurrent",
        "ConvertibleNotesPayable",
        "ConvertibleDebtNoncurrent",
        "SecuritiesLoaned",
        "PayablesToBrokerDealersAndClearingOrganizations",
    ]
    debt_proxy = sum_balance_sheet_facts(
        facts,
        tags=_debt_tags,
        quarters=quarters,
    ).rename(columns={"SUM": "Total Debt (Proxy)"})

    _found_debt = [t for t in _debt_tags if _tag_exists(facts, t, "us-gaap")]
    logger.debug("[COMPANYFACTS] Debt proxy tags present in XBRL: %s", _found_debt or 'none')
    if debt_proxy.empty or debt_proxy["Total Debt (Proxy)"].fillna(0).eq(0).all():
        logger.warning("[COMPANYFACTS] WARNING: Total Debt proxy is zero or empty. "
                       "HOOD may use non-standard liability tags not yet in the candidate list.")

    if not debt_proxy.empty:
        wide_debt = debt_proxy.set_index("quarter_end").T
        wide_debt.columns = [d.date().isoformat() for d in wide_debt.columns]
        wide_debt.insert(0, "Line Item", ["Total Debt (Proxy)"])
        wide_debt = wide_debt.reset_index(drop=True)
        bs_tbl = pd.concat([bs_tbl, wide_debt], ignore_index=True)



    tag_report = pd.concat([is_rep.assign(Statement="IS"), bs_rep.assign(Statement="BS"), cf_rep.assign(Statement="CF")], ignore_index=True)
    return is_tbl, bs_tbl, cf_tbl, tag_report


# ---------------------------------------------------------------------------
# CLI and Main Entry Point
# ---------------------------------------------------------------------------

def main():
    """Main entry point: parse arguments and run both extraction pipelines."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", type=str, default=TICKER)
    parser.add_argument("--quarters", type=int, default=10)
    args = parser.parse_args()

    ticker = args.ticker.upper().strip()
    quarters = int(args.quarters)

    cik10 = get_cik_from_ticker(ticker)
    logger.info("Ticker=%s CIK=%s Quarters=%s", ticker, cik10, quarters)

    # Statement-based segmented extraction (one row per accession)
    stmt_is, stmt_bs, stmt_cf = run_statement_pipeline(ticker, cik10, quarters)

    # CompanyFacts GAAP extraction + proxies (one row per line item, columns per quarter)
    cf_is, cf_bs, cf_cf, cf_rep = run_companyfacts_pipeline(ticker, cik10, quarters)

    # Save outputs
    stmt_is_path = DATA_DIR / f"{ticker}_stmt_IS_{quarters}Q.csv"
    stmt_bs_path = DATA_DIR / f"{ticker}_stmt_BS_{quarters}Q.csv"
    stmt_cf_path = DATA_DIR / f"{ticker}_stmt_CF_{quarters}Q.csv"

    cf_is_path = DATA_DIR / f"{ticker}_companyfacts_IS_{quarters}Q.csv"
    cf_bs_path = DATA_DIR / f"{ticker}_companyfacts_BS_{quarters}Q.csv"
    cf_cf_path = DATA_DIR / f"{ticker}_companyfacts_CF_{quarters}Q.csv"
    cf_rep_path = DATA_DIR / f"{ticker}_companyfacts_tag_report.csv"

    stmt_is.to_csv(stmt_is_path, index=False)
    stmt_bs.to_csv(stmt_bs_path, index=False)
    stmt_cf.to_csv(stmt_cf_path, index=False)

    cf_is.to_csv(cf_is_path, index=False)
    cf_bs.to_csv(cf_bs_path, index=False)
    cf_cf.to_csv(cf_cf_path, index=False)
    cf_rep.to_csv(cf_rep_path, index=False)

    logger.info("\nSaved statement-parsed CSVs:")
    logger.info("%s", stmt_is_path)
    logger.info("%s", stmt_bs_path)
    logger.info("%s", stmt_cf_path)

    logger.info("\nSaved companyfacts CSVs:")
    logger.info("%s", cf_is_path)
    logger.info("%s", cf_bs_path)
    logger.info("%s", cf_cf_path)
    logger.info("%s", cf_rep_path)

    logger.info("\nNotes:")
    logger.info("1) Assets under custody may be blank. If it is not in statements, it is a KPI in MD&A, not GAAP.")
    logger.info("2) If 'Other revenue' is noisy, tighten its regex from '\\bother\\b' to the exact label from the statement.")
    logger.info("3) Companyfacts 'Total Debt (Proxy)' is HOOD-specific: ConvertibleDebtNoncurrent + SecuritiesBorrowedLiability.")


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(message)s",
    )
    main()
