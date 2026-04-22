"""
main.py — FastAPI con supporto trimestrale, confronto periodi e trend.
"""
from __future__ import annotations
import os, tempfile, json
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware

from database import init_db, get_db, Client, FinancialReport, AccountEntry
from excel_parser import parse_excel
from ai_service import generate_commento

app = FastAPI(title="Analisi Professionisti", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def startup():
    init_db()

@app.get("/", include_in_schema=False)
def serve_frontend():
    html_path = Path(__file__).parent / "index.html"
    if not html_path.exists():
        return {"error": "index.html non trovato"}
    return FileResponse(html_path)


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

class AccountEntryOut(BaseModel):
    id: int
    codice: Optional[str]
    descrizione: str
    tipo: str
    incassi: float
    pagamenti: float
    rettifiche: float
    reddito_rettificato: Optional[float]
    class Config: from_attributes = True

class ReportOut(BaseModel):
    id: int
    client_id: str
    periodo: str
    settore: Optional[str]
    file_type: Optional[str]
    ricavi: float
    costi: float
    margine: float
    margine_percentuale: float
    commento_ai: Optional[str]
    accounts: list[AccountEntryOut] = []
    class Config: from_attributes = True

class ReportSummary(BaseModel):
    id: int
    client_id: str
    periodo: str
    settore: Optional[str]
    file_type: Optional[str]
    ricavi: float
    costi: float
    margine: float
    margine_percentuale: float
    commento_ai: Optional[str]
    class Config: from_attributes = True

class PeriodDelta(BaseModel):
    campo: str
    valore_corrente: float
    valore_precedente: float
    delta_assoluto: float
    delta_percentuale: float

class CompareResult(BaseModel):
    current: ReportSummary
    previous: Optional[ReportSummary]
    deltas: list[PeriodDelta]
    tipo_confronto: str   # "periodo_precedente" | "stesso_periodo_anno_prima"

class TrendPoint(BaseModel):
    periodo: str
    ricavi: float
    costi: float
    margine: float
    margine_percentuale: float

class BenchmarkSector(BaseModel):
    settore: str
    label: str
    n_clienti: int
    margine_pct_medio: float
    ricavi_medi: float
    costi_medi: float


# ─────────────────────────────────────────────────────────────
# Upload
# ─────────────────────────────────────────────────────────────

@app.post("/upload", response_model=ReportOut)
async def upload_file(
    file: UploadFile = File(...),
    settore:     Optional[str] = Query(None),
    periodo_override: Optional[str] = Query(None,
        description="Es: '2025', '2025-Q1'. Se non fornito, estratto dal nome file."),
    db: Session = Depends(get_db),
):
    if not file.filename:
        raise HTTPException(400, "Nessun file fornito.")
    suffix = Path(file.filename).suffix.lower()
    if suffix not in {".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(400, f"Formato non supportato: {suffix}")

    content = await file.read()
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        parsed = parse_excel(tmp_path)
        parsed["client_id"] = _client_id_from_filename(file.filename) or parsed["client_id"]
    except ValueError as e:
        raise HTTPException(422, str(e))
    finally:
        os.unlink(tmp_path)

    client_id = parsed["client_id"]
    periodo   = periodo_override or parsed["periodo"]
    settore   = settore or "altro"

    _validate_periodo(periodo)

    # Upsert client
    client = db.get(Client, client_id)
    if not client:
        client = Client(client_id=client_id, settore=settore)
        db.add(client)
    else:
        client.settore = settore
    db.flush()

    # Overwrite existing same-period report
    existing = db.query(FinancialReport).filter_by(client_id=client_id, periodo=periodo).first()
    if existing:
        db.delete(existing)
        db.flush()

    # Peer stats
    peer_stats = _compute_peer_stats(db, settore, client_id)

    # Previous period for AI trend context
    prev_periodo = _previous_period(periodo)
    prev_report  = db.query(FinancialReport).filter_by(
        client_id=client_id, periodo=prev_periodo).first()
    comparison_data = _build_comparison_data(parsed, prev_report)

    # AI commentary
    commento = generate_commento(
        client_id=client_id,
        periodo=periodo,
        ricavi=parsed["ricavi"],
        costi=parsed["costi"],
        margine=parsed["margine"],
        margine_percentuale=parsed["margine_percentuale"],
        settore=settore,
        account_breakdown=parsed["raw_accounts"],
        peer_stats=peer_stats,
        comparison_data=comparison_data,
    )

    report = FinancialReport(
        client_id=client_id,
        periodo=periodo,
        settore=settore,
        file_type=parsed.get("file_type", "prospetto_reddito"),
        ricavi=parsed["ricavi"],
        costi=parsed["costi"],
        margine=parsed["margine"],
        margine_percentuale=parsed["margine_percentuale"],
        commento_ai=commento,
    )
    db.add(report)
    db.flush()

    for acc in parsed["raw_accounts"]:
        db.add(AccountEntry(
            report_id=report.id,
            codice=acc.get("codice"),
            descrizione=acc.get("descrizione", ""),
            tipo=acc.get("tipo", "altro"),
            incassi=acc.get("incassi", 0.0),
            pagamenti=acc.get("pagamenti", 0.0),
            rettifiche=acc.get("rettifiche", 0.0),
            reddito_rettificato=acc.get("reddito_rettificato"),
        ))

    db.commit()
    db.refresh(report)
    return report


# ─────────────────────────────────────────────────────────────
# Reports
# ─────────────────────────────────────────────────────────────

@app.get("/reports", response_model=list[ReportSummary])
def list_reports(
    client_id: Optional[str] = None,
    settore:   Optional[str] = None,
    db: Session = Depends(get_db),
):
    q = db.query(FinancialReport)
    if client_id: q = q.filter_by(client_id=client_id)
    if settore:   q = q.filter_by(settore=settore)
    return q.order_by(FinancialReport.created_at.desc()).all()

@app.get("/reports/{report_id}", response_model=ReportOut)
def get_report(report_id: int, db: Session = Depends(get_db)):
    r = db.get(FinancialReport, report_id)
    if not r: raise HTTPException(404, "Report non trovato")
    return r

@app.delete("/reports/{report_id}")
def delete_report(report_id: int, db: Session = Depends(get_db)):
    r = db.get(FinancialReport, report_id)
    if not r: raise HTTPException(404, "Report non trovato")
    db.delete(r); db.commit()
    return {"deleted": report_id}


# ─────────────────────────────────────────────────────────────
# Trend & Compare
# ─────────────────────────────────────────────────────────────

@app.get("/clients/{client_id}/trend", response_model=list[TrendPoint])
def client_trend(client_id: str, db: Session = Depends(get_db)):
    """Tutti i periodi di un cliente, ordinati cronologicamente."""
    rows = (
        db.query(FinancialReport)
        .filter_by(client_id=client_id)
        .all()
    )
    rows.sort(key=lambda r: _period_sort_key(r.periodo))
    return [
        TrendPoint(
            periodo=r.periodo,
            ricavi=r.ricavi,
            costi=r.costi,
            margine=r.margine,
            margine_percentuale=r.margine_percentuale,
        )
        for r in rows
    ]

@app.get("/clients/{client_id}/compare", response_model=CompareResult)
def compare_periods(
    client_id: str,
    periodo:   str = Query(..., description="Es: '2025-Q2'"),
    tipo:      str = Query("periodo_precedente",
                           description="'periodo_precedente' o 'stesso_periodo_anno_prima'"),
    db: Session = Depends(get_db),
):
    """Confronta un periodo con il precedente o lo stesso periodo dell'anno prima."""
    current = db.query(FinancialReport).filter_by(
        client_id=client_id, periodo=periodo).first()
    if not current:
        raise HTTPException(404, f"Report per periodo '{periodo}' non trovato")

    if tipo == "stesso_periodo_anno_prima":
        ref_periodo = _same_period_prev_year(periodo)
    else:
        ref_periodo = _previous_period(periodo)

    previous = db.query(FinancialReport).filter_by(
        client_id=client_id, periodo=ref_periodo).first()

    deltas = []
    if previous:
        for campo, curr_val, prev_val in [
            ("ricavi",              current.ricavi,              previous.ricavi),
            ("costi",               current.costi,               previous.costi),
            ("margine",             current.margine,             previous.margine),
            ("margine_percentuale", current.margine_percentuale, previous.margine_percentuale),
        ]:
            delta_abs = curr_val - prev_val
            delta_pct = (delta_abs / prev_val * 100) if prev_val else 0.0
            deltas.append(PeriodDelta(
                campo=campo,
                valore_corrente=round(curr_val, 2),
                valore_precedente=round(prev_val, 2),
                delta_assoluto=round(delta_abs, 2),
                delta_percentuale=round(delta_pct, 2),
            ))

    return CompareResult(
        current=current,
        previous=previous,
        deltas=deltas,
        tipo_confronto=tipo,
    )


# ─────────────────────────────────────────────────────────────
# Clients & Settori
# ─────────────────────────────────────────────────────────────

@app.get("/clients")
def list_clients(db: Session = Depends(get_db)):
    return [{"client_id": c.client_id, "settore": c.settore} for c in db.query(Client).all()]

@app.get("/clients/{client_id}/reports", response_model=list[ReportSummary])
def client_reports(client_id: str, db: Session = Depends(get_db)):
    rows = (
        db.query(FinancialReport)
        .filter_by(client_id=client_id)
        .all()
    )
    rows.sort(key=lambda r: _period_sort_key(r.periodo), reverse=True)
    return rows

@app.get("/settori")
def get_settori():
    bench_file = Path(__file__).parent / "benchmarks.json"
    if not bench_file.exists(): return []
    raw = json.loads(bench_file.read_text())
    return [{"key": k, "label": v.get("label", k)} for k, v in raw.items()]

@app.get("/benchmarks/raw")
def benchmarks_raw():
    bench_file = Path(__file__).parent / "benchmarks.json"
    if not bench_file.exists(): return {}
    return json.loads(bench_file.read_text())

@app.get("/benchmarks/sectors", response_model=list[BenchmarkSector])
def benchmark_sectors(db: Session = Depends(get_db)):
    rows = (
        db.query(
            FinancialReport.settore,
            func.count(FinancialReport.id).label("n"),
            func.avg(FinancialReport.margine_percentuale).label("mp"),
            func.avg(FinancialReport.ricavi).label("rv"),
            func.avg(FinancialReport.costi).label("co"),
        ).group_by(FinancialReport.settore).all()
    )
    bench_file = Path(__file__).parent / "benchmarks.json"
    labels = {}
    if bench_file.exists():
        raw = json.loads(bench_file.read_text())
        labels = {k: v.get("label", k) for k, v in raw.items()}
    return [
        BenchmarkSector(
            settore=r.settore or "altro",
            label=labels.get(r.settore or "altro", r.settore or "altro"),
            n_clienti=r.n,
            margine_pct_medio=round(r.mp or 0, 2),
            ricavi_medi=round(r.rv or 0, 2),
            costi_medi=round(r.co or 0, 2),
        )
        for r in rows if r.settore
    ]

@app.get("/health")
def health(): return {"status": "ok"}


# ─────────────────────────────────────────────────────────────
# Period helpers
# ─────────────────────────────────────────────────────────────

def _validate_periodo(p: str):
    import re
    if re.match(r"^\d{4}$", p): return
    if re.match(r"^\d{4}-Q[1-4]$", p): return
    raise HTTPException(400, f"Formato periodo non valido: '{p}'. Usa '2025' o '2025-Q1'.")

def _previous_period(p: str) -> str:
    if "-Q" in p:
        year, q = p.split("-Q"); q = int(q)
        return f"{int(year)-1}-Q4" if q == 1 else f"{year}-Q{q-1}"
    return str(int(p) - 1)

def _same_period_prev_year(p: str) -> str:
    if "-Q" in p:
        year, q = p.split("-Q")
        return f"{int(year)-1}-Q{q}"
    return str(int(p) - 1)

def _period_sort_key(p: str) -> tuple:
    if "-Q" in p:
        year, q = p.split("-Q")
        return (int(year), int(q))
    return (int(p), 0)

def _client_id_from_filename(filename: str) -> str | None:
    import re
    m = re.match(r"^(\d+)", Path(filename).stem)
    return m.group(1) if m else None

def _compute_peer_stats(db: Session, settore: str, exclude_client_id: str):
    rows = (
        db.query(FinancialReport)
        .filter(FinancialReport.settore == settore)
        .filter(FinancialReport.client_id != exclude_client_id)
        .all()
    )
    if len(rows) < 2: return None
    return {
        "count":             len(rows),
        "margine_pct_medio": round(sum(r.margine_percentuale for r in rows) / len(rows), 2),
        "ricavi_medi":       round(sum(r.ricavi for r in rows) / len(rows), 2),
        "costi_medi":        round(sum(r.costi for r in rows) / len(rows), 2),
    }

def _build_comparison_data(parsed: dict, prev: FinancialReport | None) -> dict | None:
    if not prev: return None
    r, c, m, mp = parsed["ricavi"], parsed["costi"], parsed["margine"], parsed["margine_percentuale"]
    return {
        "periodo_precedente": prev.periodo,
        "delta_ricavi_pct":   round((r - prev.ricavi)  / prev.ricavi  * 100, 1) if prev.ricavi  else 0,
        "delta_costi_pct":    round((c - prev.costi)   / prev.costi   * 100, 1) if prev.costi   else 0,
        "delta_margine_pct":  round((m - prev.margine) / prev.margine * 100, 1) if prev.margine else 0,
        "delta_margine_pp":   round(mp - prev.margine_percentuale, 1),
        "ricavi_prec":        prev.ricavi,
        "costi_prec":         prev.costi,
        "margine_prec":       prev.margine,
        "margine_pct_prec":   prev.margine_percentuale,
    }
