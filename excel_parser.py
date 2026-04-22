"""
parser.py — Robust Excel parser for Prospetto Reddito files exported from accounting software.

File structure (discovered from real sample):
  - Sheet: Foglio1
  - Columns: Descrizione | COLONNA2 | Incassi | Pagamenti | Rettifiche | I/P | % reddito | Reddito rettificato | % compensi
  - Row 1: header
  - Rows 2+: accounting entries with hierarchical indentation (spaces in description)
  - Top-level rows (no leading spaces) = account groups (e.g. "105 - Compensi professionali")
  - Indented rows = sub-accounts

Revenue accounts: Incassi column > 0
Cost accounts   : Pagamenti column > 0  (or Rettifiche for accruals/amortisation)
Final income     : "Reddito rettificato" column on top-level rows with % reddito == 100
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import openpyxl
import pandas as pd


# ---------------------------------------------------------------------------
# Constants / keyword patterns
# ---------------------------------------------------------------------------

REVENUE_KEYWORDS = re.compile(
    r"compensi|ricavi|proventi|corrispettivi|fatturato|incassi totali",
    re.IGNORECASE,
)
COST_KEYWORDS = re.compile(
    r"costi?|spese?|pagamenti|oneri|contributi|ammort|retribuz|consulenz|canoni|acquisto",
    re.IGNORECASE,
)

HEADER_ALIASES = {
    "incassi": "incassi",
    "pagamenti": "pagamenti",
    "rettifiche": "rettifiche",
    "reddito rettificato": "reddito_rettificato",
    "% reddito": "perc_reddito",
    "% compensi": "perc_compensi",
    "rag.fiscale": "descrizione",
    "conto": "descrizione",
    "partitario": "descrizione",
    "descrizione": "descrizione",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_header(raw: str) -> str:
    raw = str(raw).strip().lower()
    for pattern, canonical in HEADER_ALIASES.items():
        if raw.startswith(pattern):
            return canonical
    return raw.replace(" ", "_").replace("/", "_").replace(".", "_")


def _indent_level(text: str | None) -> int:
    """Count leading spaces as a proxy for hierarchy depth (4 spaces = 1 level)."""
    if not text:
        return 0
    stripped = str(text).lstrip()
    return (len(str(text)) - len(stripped)) // 4


def _is_top_level(text: str | None) -> bool:
    return _indent_level(text) == 0


def _extract_account_code(text: str) -> str | None:
    m = re.match(r"^\s*(\d{2,4})\s*[-–]", str(text))
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------


def parse_excel(file_path: str | Path) -> dict[str, Any]:
    """
    Parse an accounting Excel file and return structured financial data.

    Returns
    -------
    dict with keys:
        client_id, periodo, ricavi, costi, margine, margine_percentuale,
        raw_accounts (list of dicts for each top-level account group)
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Derive client_id from filename (e.g. "401_ProspettoReddito2025.XLSX" → "401")
    client_id = _extract_client_id(file_path.stem)
    anno = _extract_year(file_path.stem)

    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = _find_sheet(wb)

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        raise ValueError("Empty worksheet")

    # ----- Locate header row (first non-empty row) -----
    header_idx = _find_header_row(rows)
    raw_headers = [str(c).strip() if c is not None else "" for c in rows[header_idx]]
    col_map = {_normalise_header(h): i for i, h in enumerate(raw_headers) if h}

    _require_columns(col_map, ["incassi", "pagamenti", "descrizione"])

    ci = col_map["incassi"]
    cp = col_map["pagamenti"]
    cd = col_map["descrizione"]
    cr = col_map.get("rettifiche")
    crr = col_map.get("reddito_rettificato")

    # ----- Parse data rows -----
    ricavi_totale = 0.0
    costi_totale = 0.0
    raw_accounts: list[dict] = []

    data_rows = rows[header_idx + 1 :]

    for row in data_rows:
        desc = row[cd]
        if desc is None:
            continue

        desc_str = str(desc)
        if not _is_top_level(desc_str):
            continue  # skip sub-accounts for top-level aggregation

        incassi = _num(row[ci])
        pagamenti = _num(row[cp])
        rettifiche = _num(row[cr]) if cr is not None else 0.0
        reddito_rett = _num(row[crr]) if crr is not None else None

        account_code = _extract_account_code(desc_str)
        account_name = desc_str.strip()

        entry = {
            "codice": account_code,
            "descrizione": account_name,
            "incassi": incassi,
            "pagamenti": pagamenti,
            "rettifiche": rettifiche,
            "reddito_rettificato": reddito_rett,
        }

        if incassi > 0:
            ricavi_totale += incassi
            entry["tipo"] = "ricavo"
        elif pagamenti > 0 or abs(rettifiche) > 0:
            costi_totale += pagamenti + abs(rettifiche)
            entry["tipo"] = "costo"
        else:
            entry["tipo"] = "altro"

        raw_accounts.append(entry)

    margine = ricavi_totale - costi_totale
    margine_percentuale = (margine / ricavi_totale * 100) if ricavi_totale else 0.0

    return {
        "client_id": client_id,
        "periodo": f"{anno}",
        "ricavi": round(ricavi_totale, 2),
        "costi": round(costi_totale, 2),
        "margine": round(margine, 2),
        "margine_percentuale": round(margine_percentuale, 2),
        "raw_accounts": raw_accounts,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_client_id(stem: str) -> str:
    """Extract numeric client id from filename stem (e.g. '401_Prospetto...' → '401')."""
    m = re.match(r"^(\d+)", stem)
    return m.group(1) if m else "UNKNOWN"


def _extract_year(stem: str) -> str:
    m = re.search(r"(20\d{2})", stem)
    return m.group(1) if m else "UNKNOWN"


def _find_sheet(wb: openpyxl.Workbook) -> Any:
    """Return the most likely data sheet."""
    preferred = ["foglio1", "sheet1", "prospetto", "reddito", "dati"]
    for name in wb.sheetnames:
        if name.lower() in preferred:
            return wb[name]
    return wb.active  # fallback


def _find_header_row(rows: list) -> int:
    """Find the row index that looks like a header (contains text keywords)."""
    header_signals = {"incassi", "pagamenti", "rettifiche", "descrizione", "conto", "reddito"}
    for i, row in enumerate(rows):
        cells = {str(c).strip().lower() for c in row if c is not None}
        if cells & header_signals:
            return i
    return 0  # fallback: first row


def _require_columns(col_map: dict, required: list[str]) -> None:
    missing = [c for c in required if c not in col_map]
    if missing:
        raise ValueError(f"Colonne mancanti nel file: {missing}. Trovate: {list(col_map.keys())}")


def _num(val: Any) -> float:
    """Safe numeric coercion."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0
