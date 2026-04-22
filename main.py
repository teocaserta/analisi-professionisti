"""
main.py v4 — Supporto file singolo + confronto libero tra due periodi.
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
from ai_service import generate_commento, generate_commento_confronto

app = FastAPI(title="Analisi Professionisti", version="4.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def startup():
    init_db()

@app.get("/", include_in_schema=False)
def serve_frontend():
    p = Path(__file__).parent / "index.html"
    return FileResponse(p) if p.exists() else {"error": "index.html non trovato"}


# ─────────────────────────────────────────────────────────────
# Pydantic
# ─────────────────────────────────────────────────────────────

class AccountEntryOut(BaseModel):
    id: int; codice: Optional[str]; descrizione: str; tipo: str
    incassi: float; pagamenti: float; rettifiche: float
    reddito_rettificato: Optional[float]
    class Config: from_attributes = True

class ReportOut(BaseModel):
    id: int; client_id: str; periodo: str; settore: Optional[str]
    file_type: Optional[str]; mesi_periodo: int
    ricavi: float; costi: float; margine: float; margine_percentuale: float
    ricavi_annualizzati: float
    commento_ai: Optional[str]
    accounts: list[AccountEntryOut] = []
    class Config: from_attributes = True

class ReportSummary(BaseModel):
    id: int; client_id: str; periodo: str; settore: Optional[str]
    file_type: Optional[str]; mesi_periodo: int
    ricavi: float; costi: float; margine: float; margine_percentuale: float
    ricavi_annualizzati: float; commento_ai: Optional[str]
    class Config: from_attributes = True

class PeriodDelta(BaseModel):
    campo: str; valore_a: float; valore_b: float
    delta_assoluto: float; delta_percentuale: float

class CompareUploadResult(BaseModel):
    report_a: ReportOut; report_b: ReportOut
    deltas: list[PeriodDelta]; commento_confronto: str

class TrendPoint(BaseModel):
    periodo: str; ricavi: float; costi: float
    margine: float; margine_percentuale: float; mesi_periodo: int

class BenchmarkSector(BaseModel):
    settore: str; label: str; n_clienti: int
    margine_pct_medio: float; ricavi_medi: float; costi_medi: float


# ─────────────────────────────────────────────────────────────
# Upload singolo
# ─────────────────────────────────────────────────────────────

@app.post("/upload", response_model=ReportOut)
async def upload_file(
    file: UploadFile = File(...),
    settore:          Optional[str] = Query(None),
    periodo_override: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    parsed, client_id, periodo = await _parse_upload(file, periodo_override)
    settore = settore or "altro"
    _validate_periodo(periodo)
    mesi = _mesi_periodo(periodo)

    client = db.get(Client, client_id)
    if not client:
        client = Client(client_id=client_id, settore=settore); db.add(client)
    else:
        client.settore = settore
    db.flush()

    existing = db.query(FinancialReport).filter_by(client_id=client_id, periodo=periodo).first()
    if existing: db.delete(existing); db.flush()

    peer_stats = _compute_peer_stats(db, settore, client_id)
    prev_report = db.query(FinancialReport).filter_by(
        client_id=client_id, periodo=_previous_period(periodo)).first()
    comparison_data = _build_comparison_data(parsed, prev_report)

    commento = generate_commento(
        client_id=client_id, periodo=periodo,
        ricavi=parsed["ricavi"], costi=parsed["costi"],
        margine=parsed["margine"], margine_percentuale=parsed["margine_percentuale"],
        mesi_periodo=mesi,
        settore=settore, account_breakdown=parsed["raw_accounts"],
        peer_stats=peer_stats, comparison_data=comparison_data,
    )

    report = _save_report(db, client_id, periodo, settore,
                          parsed, mesi, commento)
    db.commit(); db.refresh(report)
    _ = report.accounts  # eagerly load before session closes
    return report


# ─────────────────────────────────────────────────────────────
# Upload confronto (2 file)
# ─────────────────────────────────────────────────────────────

@app.post("/compare-upload", response_model=CompareUploadResult)
async def compare_upload(
    file_a:    UploadFile = File(...),
    file_b:    UploadFile = File(...),
    periodo_a: str = Query(...),
    periodo_b: str = Query(...),
    settore:   str = Query("altro"),
    db: Session = Depends(get_db),
):
    _validate_periodo(periodo_a); _validate_periodo(periodo_b)

    parsed_a, cid_a, _ = await _parse_upload(file_a, periodo_a)
    parsed_b, cid_b, _ = await _parse_upload(file_b, periodo_b)

    # Usa client_id del file A (stesso cliente)
    client_id = cid_a
    mesi_a = _mesi_periodo(periodo_a)
    mesi_b = _mesi_periodo(periodo_b)

    # Upsert client
    client = db.get(Client, client_id)
    if not client:
        client = Client(client_id=client_id, settore=settore); db.add(client)
    else:
        client.settore = settore
    db.flush()

    # Salva entrambi (sovrascrive se stesso periodo)
    for pid, parsed, mesi in [(periodo_a, parsed_a, mesi_a), (periodo_b, parsed_b, mesi_b)]:
        ex = db.query(FinancialReport).filter_by(client_id=client_id, periodo=pid).first()
        if ex: db.delete(ex); db.flush()

    peer_stats = _compute_peer_stats(db, settore, client_id)
    benchmarks = _load_benchmarks()
    bench = benchmarks.get(settore, benchmarks.get("altro", {}))

    # Commento individuale per A
    comm_a = generate_commento(
        client_id=client_id, periodo=periodo_a,
        ricavi=parsed_a["ricavi"], costi=parsed_a["costi"],
        margine=parsed_a["margine"], margine_percentuale=parsed_a["margine_percentuale"],
        mesi_periodo=mesi_a, settore=settore,
        account_breakdown=parsed_a["raw_accounts"],
        peer_stats=peer_stats,
    )
    # Commento individuale per B
    comm_b = generate_commento(
        client_id=client_id, periodo=periodo_b,
        ricavi=parsed_b["ricavi"], costi=parsed_b["costi"],
        margine=parsed_b["margine"], margine_percentuale=parsed_b["margine_percentuale"],
        mesi_periodo=mesi_b, settore=settore,
        account_breakdown=parsed_b["raw_accounts"],
        peer_stats=peer_stats,
    )
    # Commento confronto
    comm_confronto = generate_commento_confronto(
        periodo_a=periodo_a, parsed_a=parsed_a, mesi_a=mesi_a,
        periodo_b=periodo_b, parsed_b=parsed_b, mesi_b=mesi_b,
        settore=settore, bench=bench,
    )

    report_a = _save_report(db, client_id, periodo_a, settore, parsed_a, mesi_a, comm_a)
    report_b = _save_report(db, client_id, periodo_b, settore, parsed_b, mesi_b, comm_b)
    db.commit(); db.refresh(report_a); db.refresh(report_b)
    _ = report_a.accounts; _ = report_b.accounts  # eagerly load

    deltas = _compute_deltas(parsed_a, parsed_b)

    return CompareUploadResult(
        report_a=report_a, report_b=report_b,
        deltas=deltas, commento_confronto=comm_confronto,
    )


# ─────────────────────────────────────────────────────────────
# Read endpoints
# ─────────────────────────────────────────────────────────────

@app.get("/reports", response_model=list[ReportSummary])
def list_reports(client_id: Optional[str]=None, settore: Optional[str]=None,
                 db: Session=Depends(get_db)):
    q = db.query(FinancialReport)
    if client_id: q = q.filter_by(client_id=client_id)
    if settore:   q = q.filter_by(settore=settore)
    return q.order_by(FinancialReport.created_at.desc()).all()

@app.get("/reports/{report_id}", response_model=ReportOut)
def get_report(report_id: int, db: Session=Depends(get_db)):
    r = db.get(FinancialReport, report_id)
    if not r: raise HTTPException(404, "Report non trovato")
    return r

@app.delete("/reports/{report_id}")
def delete_report(report_id: int, db: Session=Depends(get_db)):
    r = db.get(FinancialReport, report_id)
    if not r: raise HTTPException(404, "Report non trovato")
    db.delete(r); db.commit()
    return {"deleted": report_id}

@app.get("/clients")
def list_clients(db: Session=Depends(get_db)):
    return [{"client_id": c.client_id, "settore": c.settore}
            for c in db.query(Client).all()]

@app.get("/clients/{client_id}/reports", response_model=list[ReportSummary])
def client_reports(client_id: str, db: Session=Depends(get_db)):
    rows = db.query(FinancialReport).filter_by(client_id=client_id).all()
    rows.sort(key=lambda r: _period_sort_key(r.periodo), reverse=True)
    return rows

@app.get("/clients/{client_id}/trend", response_model=list[TrendPoint])
def client_trend(client_id: str, db: Session=Depends(get_db)):
    rows = db.query(FinancialReport).filter_by(client_id=client_id).all()
    rows.sort(key=lambda r: _period_sort_key(r.periodo))
    return [TrendPoint(periodo=r.periodo, ricavi=r.ricavi, costi=r.costi,
                       margine=r.margine, margine_percentuale=r.margine_percentuale,
                       mesi_periodo=r.mesi_periodo or 12) for r in rows]

@app.get("/settori")
def get_settori():
    f = Path(__file__).parent / "benchmarks.json"
    if not f.exists(): return []
    return [{"key": k, "label": v.get("label", k)}
            for k, v in json.loads(f.read_text()).items()]

@app.get("/benchmarks/raw")
def benchmarks_raw():
    f = Path(__file__).parent / "benchmarks.json"
    return json.loads(f.read_text()) if f.exists() else {}

@app.get("/benchmarks/sectors", response_model=list[BenchmarkSector])
def benchmark_sectors(db: Session=Depends(get_db)):
    rows = db.query(
        FinancialReport.settore,
        func.count(FinancialReport.id).label("n"),
        func.avg(FinancialReport.margine_percentuale).label("mp"),
        func.avg(FinancialReport.ricavi).label("rv"),
        func.avg(FinancialReport.costi).label("co"),
    ).group_by(FinancialReport.settore).all()
    f = Path(__file__).parent / "benchmarks.json"
    labels = {k: v.get("label",k) for k,v in json.loads(f.read_text()).items()} if f.exists() else {}
    return [BenchmarkSector(settore=r.settore or "altro",
                            label=labels.get(r.settore or "altro", r.settore or ""),
                            n_clienti=r.n,
                            margine_pct_medio=round(r.mp or 0, 2),
                            ricavi_medi=round(r.rv or 0, 2),
                            costi_medi=round(r.co or 0, 2))
            for r in rows if r.settore]

@app.get("/health")
def health(): return {"status": "ok"}


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

async def _parse_upload(file: UploadFile, periodo_override: str | None):
    suffix = Path(file.filename).suffix.lower()
    if suffix not in {".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(400, f"Formato non supportato: {suffix}")
    content = await file.read()
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(content); tmp_path = tmp.name
    try:
        parsed = parse_excel(tmp_path)
        client_id = (_client_id_from_filename(file.filename) or parsed["client_id"])
        periodo   = periodo_override or parsed["periodo"]
        return parsed, client_id, periodo
    except ValueError as e:
        raise HTTPException(422, str(e))
    finally:
        os.unlink(tmp_path)

def _save_report(db, client_id, periodo, settore, parsed, mesi, commento):
    ricavi = parsed["ricavi"]
    ra = round(ricavi * (12 / mesi), 2)
    r = FinancialReport(
        client_id=client_id, periodo=periodo, settore=settore,
        file_type=parsed.get("file_type","prospetto_reddito"),
        mesi_periodo=mesi,
        ricavi=parsed["ricavi"], costi=parsed["costi"],
        margine=parsed["margine"],
        margine_percentuale=parsed["margine_percentuale"],
        ricavi_annualizzati=ra,
        commento_ai=commento,
    )
    db.add(r); db.flush()
    for acc in parsed["raw_accounts"]:
        db.add(AccountEntry(
            report_id=r.id, codice=acc.get("codice"),
            descrizione=acc.get("descrizione",""), tipo=acc.get("tipo","altro"),
            incassi=acc.get("incassi",0.), pagamenti=acc.get("pagamenti",0.),
            rettifiche=acc.get("rettifiche",0.),
            reddito_rettificato=acc.get("reddito_rettificato"),
        ))
    return r

def _compute_deltas(a: dict, b: dict) -> list[PeriodDelta]:
    deltas = []
    for campo, va, vb in [
        ("ricavi",              a["ricavi"],              b["ricavi"]),
        ("costi",               a["costi"],               b["costi"]),
        ("margine",             a["margine"],             b["margine"]),
        ("margine_percentuale", a["margine_percentuale"], b["margine_percentuale"]),
    ]:
        da = vb - va
        dp = (da / va * 100) if va else 0.
        deltas.append(PeriodDelta(campo=campo, valore_a=round(va,2), valore_b=round(vb,2),
                                  delta_assoluto=round(da,2), delta_percentuale=round(dp,2)))
    return deltas

def _load_benchmarks():
    f = Path(__file__).parent / "benchmarks.json"
    return json.loads(f.read_text()) if f.exists() else {}

def _validate_periodo(p: str):
    import re
    if re.match(r"^\d{4}$", p) or re.match(r"^\d{4}-Q[1-4]$", p): return
    raise HTTPException(400, f"Periodo non valido: '{p}'. Usa '2025' o '2025-Q1'.")

def _mesi_periodo(p: str) -> int:
    return 3 if "-Q" in p else 12

def _previous_period(p: str) -> str:
    if "-Q" in p:
        y, q = p.split("-Q"); q = int(q)
        return f"{int(y)-1}-Q4" if q==1 else f"{y}-Q{q-1}"
    return str(int(p)-1)

def _period_sort_key(p: str) -> tuple:
    if "-Q" in p:
        y, q = p.split("-Q"); return (int(y), int(q))
    return (int(p), 0)

def _client_id_from_filename(filename: str) -> str | None:
    import re
    m = re.match(r"^(\d+)", Path(filename).stem)
    return m.group(1) if m else None

def _compute_peer_stats(db, settore, exclude):
    rows = db.query(FinancialReport).filter(
        FinancialReport.settore==settore,
        FinancialReport.client_id!=exclude).all()
    if len(rows) < 2: return None
    return {"count": len(rows),
            "margine_pct_medio": round(sum(r.margine_percentuale for r in rows)/len(rows),2),
            "ricavi_medi":       round(sum(r.ricavi for r in rows)/len(rows),2),
            "costi_medi":        round(sum(r.costi for r in rows)/len(rows),2)}

def _build_comparison_data(parsed, prev):
    if not prev: return None
    r,c,m,mp = parsed["ricavi"],parsed["costi"],parsed["margine"],parsed["margine_percentuale"]
    return {"periodo_precedente": prev.periodo,
            "delta_ricavi_pct":  round((r-prev.ricavi)/prev.ricavi*100,1) if prev.ricavi else 0,
            "delta_costi_pct":   round((c-prev.costi)/prev.costi*100,1)   if prev.costi  else 0,
            "delta_margine_pct": round((m-prev.margine)/prev.margine*100,1) if prev.margine else 0,
            "delta_margine_pp":  round(mp-prev.margine_percentuale,1),
            "ricavi_prec": prev.ricavi, "costi_prec": prev.costi,
            "margine_prec": prev.margine, "margine_pct_prec": prev.margine_percentuale}
