"""
main.py — FastAPI backend con supporto settore, benchmark e confronto peer.
"""
from __future__ import annotations
import os, tempfile, json
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from database import init_db, get_db, Client, FinancialReport, AccountEntry
from parser import parse_excel
from ai_service import generate_commento

app = FastAPI(title="Analisi Professionisti", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def startup():
    init_db()

@app.get("/", include_in_schema=False)
def serve_frontend():
    """Serve the frontend HTML directly from Railway."""
    html_path = Path(__file__).parent / "index.html"
    if not html_path.exists():
        return {"error": "index.html non trovato nella cartella del backend"}
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
    ricavi: float
    costi: float
    margine: float
    margine_percentuale: float
    commento_ai: Optional[str]
    class Config: from_attributes = True

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
    settore: Optional[str] = Query(None, description="es: medico, avvocato, dentista…"),
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
    periodo   = parsed["periodo"]
    settore   = settore or "altro"

    # Upsert client
    client = db.get(Client, client_id)
    if not client:
        client = Client(client_id=client_id, settore=settore)
        db.add(client)
    else:
        client.settore = settore
    db.flush()

    # Remove existing report for same period
    existing = db.query(FinancialReport).filter_by(client_id=client_id, periodo=periodo).first()
    if existing:
        db.delete(existing)
        db.flush()

    # Peer stats (altri clienti stesso settore, escluso questo)
    peer_stats = _compute_peer_stats(db, settore, client_id)

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
    )

    # Save report
    report = FinancialReport(
        client_id=client_id,
        periodo=periodo,
        settore=settore,
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
# Read endpoints
# ─────────────────────────────────────────────────────────────

@app.get("/reports", response_model=list[ReportSummary])
def list_reports(client_id: Optional[str] = None, settore: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(FinancialReport)
    if client_id: q = q.filter_by(client_id=client_id)
    if settore:   q = q.filter_by(settore=settore)
    return q.order_by(FinancialReport.created_at.desc()).all()

@app.get("/reports/{report_id}", response_model=ReportOut)
def get_report(report_id: int, db: Session = Depends(get_db)):
    report = db.get(FinancialReport, report_id)
    if not report: raise HTTPException(404, "Report non trovato")
    return report

@app.delete("/reports/{report_id}")
def delete_report(report_id: int, db: Session = Depends(get_db)):
    report = db.get(FinancialReport, report_id)
    if not report: raise HTTPException(404, "Report non trovato")
    db.delete(report); db.commit()
    return {"deleted": report_id}

@app.get("/clients")
def list_clients(db: Session = Depends(get_db)):
    return [{"client_id": c.client_id, "settore": c.settore} for c in db.query(Client).all()]

@app.get("/clients/{client_id}/reports", response_model=list[ReportSummary])
def client_reports(client_id: str, db: Session = Depends(get_db)):
    return db.query(FinancialReport).filter_by(client_id=client_id).order_by(FinancialReport.periodo.desc()).all()

@app.get("/benchmarks/sectors", response_model=list[BenchmarkSector])
def benchmark_sectors(db: Session = Depends(get_db)):
    """Statistiche aggregate per settore dai dati reali nel DB."""
    rows = (
        db.query(
            FinancialReport.settore,
            func.count(FinancialReport.id).label("n"),
            func.avg(FinancialReport.margine_percentuale).label("mp"),
            func.avg(FinancialReport.ricavi).label("rv"),
            func.avg(FinancialReport.costi).label("co"),
        )
        .group_by(FinancialReport.settore)
        .all()
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

@app.get("/settori")
def get_settori():
    """Lista settori disponibili dal file benchmarks.json."""
    bench_file = Path(__file__).parent / "benchmarks.json"
    if not bench_file.exists():
        return []
    raw = json.loads(bench_file.read_text())
    return [{"key": k, "label": v.get("label", k)} for k, v in raw.items()]


@app.get("/benchmarks/raw")
def benchmarks_raw():
    """Restituisce il file benchmarks.json completo per il frontend."""
    bench_file = Path(__file__).parent / "benchmarks.json"
    if not bench_file.exists():
        return {}
    return json.loads(bench_file.read_text())

@app.get("/health")
def health(): return {"status": "ok"}


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _client_id_from_filename(filename: str) -> str | None:
    import re
    m = re.match(r"^(\d+)", Path(filename).stem)
    return m.group(1) if m else None

def _compute_peer_stats(db: Session, settore: str, exclude_client_id: str) -> dict | None:
    """Calcola medie anonime degli altri clienti dello stesso settore."""
    rows = (
        db.query(FinancialReport)
        .filter(FinancialReport.settore == settore)
        .filter(FinancialReport.client_id != exclude_client_id)
        .all()
    )
    if len(rows) < 2:
        return None
    return {
        "count": len(rows),
        "margine_pct_medio": round(sum(r.margine_percentuale for r in rows) / len(rows), 2),
        "ricavi_medi":       round(sum(r.ricavi for r in rows) / len(rows), 2),
        "costi_medi":        round(sum(r.costi for r in rows) / len(rows), 2),
    }
