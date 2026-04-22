"""
ai_service.py — Commento AI con benchmark ISA 2025/2026 e confronto peer.
"""
from __future__ import annotations
import os, json, httpx
from pathlib import Path

FORFETTARIO_SOGLIA = 85000
_BENCH_PATH = Path(__file__).parent / "benchmarks.json"


def _load_benchmarks() -> dict:
    try:
        return json.loads(_BENCH_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def generate_commento(
    client_id: str,
    periodo: str,
    ricavi: float,
    costi: float,
    margine: float,
    margine_percentuale: float,
    settore: str | None = None,
    account_breakdown: list[dict] | None = None,
    peer_stats: dict | None = None,
) -> str:
    benchmarks = _load_benchmarks()
    bench = benchmarks.get(settore or "altro", benchmarks.get("altro", {}))

    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        return _openai_comment(api_key, client_id, periodo, ricavi, costi, margine,
                               margine_percentuale, bench, account_breakdown, peer_stats)
    return _rule_based_comment(ricavi, costi, margine, margine_percentuale,
                               bench, account_breakdown, peer_stats)


# ─────────────────────────────────────────────────────────────
# OpenAI path
# ─────────────────────────────────────────────────────────────

def _openai_comment(api_key, client_id, periodo, ricavi, costi, margine,
                    margine_percentuale, bench, account_breakdown, peer_stats):
    top_costs = []
    if account_breakdown:
        costs = sorted(
            [a for a in account_breakdown if a.get("tipo") == "costo"],
            key=lambda x: x.get("pagamenti", 0) + abs(x.get("rettifiche", 0)),
            reverse=True,
        )
        top_costs = [{"descrizione": c["descrizione"], "importo": c["pagamenti"]} for c in costs[:8]]

    # Calcola costo lavoro reale
    costo_lavoro_pct = _calc_costo_lavoro_pct(ricavi, account_breakdown or [])
    costo_autonomi_pct = _calc_costo_autonomi_pct(ricavi, account_breakdown or [])

    # Costruisci contesto benchmark
    bench_ctx = _build_bench_context(bench, margine_percentuale, costo_lavoro_pct, costo_autonomi_pct, ricavi)
    peer_ctx  = _build_peer_context(peer_stats)
    forf_ctx  = _build_forfettario_context(ricavi, bench)

    payload = {
        "periodo": periodo,
        "ricavi": ricavi,
        "costi": costi,
        "margine": margine,
        "margine_percentuale_sui_ricavi": margine_percentuale,
        "costo_lavoro_pct_calcolato": round(costo_lavoro_pct, 1),
        "costo_collaboratori_autonomi_pct": round(costo_autonomi_pct, 1),
        "top_voci_di_costo": top_costs,
    }

    prompt = f"""Sei un commercialista esperto italiano con specializzazione in analisi finanziaria di studi professionali. Analizza i dati e produci un commento strutturato in QUATTRO sezioni.

{bench_ctx}

{peer_ctx}

{forf_ctx}

Dati del cliente:
{json.dumps(payload, indent=2, ensure_ascii=False)}

ISTRUZIONI PRECISE:
1. analisi_generale: confronta esplicitamente margine %, costo lavoro e ricavi con i benchmark ISA del settore. Indica se il professionista è sopra/sotto/in linea con i peer reali (se disponibili). Cita i range ISA.
2. punti_attenzione: usa gli alert specifici del settore se il costo lavoro è fuori range. Sii diretto e tecnico.
3. ottimizzazione_fiscale: includi le note fiscali specifiche del settore, il regime forfettario se applicabile, e suggerimenti concreti basati sulle voci di costo rilevate.
4. raccomandazioni: azioni prioritarie con orizzonte temporale (entro fine anno, nel prossimo trimestre, ecc.).

Non inventare dati. Non citare il cliente per nome. Massimo 5 frasi per sezione.

Rispondi SOLO con JSON valido, nessun testo fuori:
{{"analisi_generale":"...","punti_attenzione":"...","ottimizzazione_fiscale":"...","raccomandazioni":"..."}}"""

    try:
        response = httpx.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1000,
                "temperature": 0.35,
            },
            timeout=35,
        )
        response.raise_for_status()
        raw = response.json()["choices"][0]["message"]["content"].strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.dumps(json.loads(raw), ensure_ascii=False)
    except Exception:
        return _rule_based_comment(ricavi, costi, margine, margine_percentuale,
                                   bench, account_breakdown, peer_stats)


# ─────────────────────────────────────────────────────────────
# Rule-based fallback
# ─────────────────────────────────────────────────────────────

def _rule_based_comment(ricavi, costi, margine, margine_percentuale,
                        bench, account_breakdown, peer_stats):
    return json.dumps(
        _rule_based_structured(ricavi, costi, margine, margine_percentuale,
                               bench, account_breakdown, peer_stats),
        ensure_ascii=False,
    )


def _rule_based_structured(ricavi, costi, margine, margine_percentuale,
                            bench, account_breakdown, peer_stats):
    cost_ratio = (costi / ricavi * 100) if ricavi else 0
    costo_lavoro_pct   = _calc_costo_lavoro_pct(ricavi, account_breakdown or [])
    costo_autonomi_pct = _calc_costo_autonomi_pct(ricavi, account_breakdown or [])

    label      = bench.get("label", "settore") if bench else "settore"
    bm         = bench.get("margine_pct_medio") if bench else None
    bmin       = bench.get("margine_pct_min") if bench else None
    bmax       = bench.get("margine_pct_max") if bench else None
    lav_min    = bench.get("costo_lavoro_min") if bench else None
    lav_max    = bench.get("costo_lavoro_max") if bench else None
    isa_codice = bench.get("isa_codice", "") if bench else ""

    # ── Analisi generale ──
    if bm:
        diff = margine_percentuale - bm
        if diff > 5:
            vs_bench = f"superiore di {diff:.1f}pp alla media ISA {label} ({bm:.0f}%)"
        elif diff < -5:
            vs_bench = f"inferiore di {abs(diff):.1f}pp alla media ISA {label} ({bm:.0f}%)"
        else:
            vs_bench = f"in linea con la media ISA {label} ({bm:.0f}%)"
    else:
        vs_bench = "nella media del settore"

    analisi = (
        f"Il periodo evidenzia ricavi pari a €{ricavi:,.0f} con un margine operativo "
        f"del {margine_percentuale:.1f}%, {vs_bench}"
        f"{f' (range ISA: {bmin}–{bmax}%)' if bmin and bmax else ''}. "
        f"Il costo del lavoro incide per il {costo_lavoro_pct:.1f}% sui ricavi"
        f"{f', rispetto al range di settore {lav_min}–{lav_max}%' if lav_min and lav_max else ''}."
    )

    if peer_stats and peer_stats.get("count", 0) >= 2:
        pdiff = margine_percentuale - peer_stats["margine_pct_medio"]
        analisi += (
            f" Rispetto ai {peer_stats['count']} clienti simili nel portafoglio, "
            f"il margine risulta {'superiore' if pdiff >= 0 else 'inferiore'} di {abs(pdiff):.1f}pp "
            f"(media peer: {peer_stats['margine_pct_medio']:.1f}%)."
        )

    if bmin and margine_percentuale < bmin:
        analisi += f" ⚠ Il margine è sotto la soglia minima ISA per {label} ({bmin}%): situazione da monitorare."
    elif bmax and margine_percentuale > bmax:
        analisi += f" ✓ Il margine supera la soglia massima ISA per {label} ({bmax}%): performance eccellente."

    # ── Punti di attenzione (basati su alert ISA specifici) ──
    attenzione_parts = []

    if bench and lav_min and lav_max:
        if costo_lavoro_pct > lav_max:
            alert = bench.get("alert_costo_lavoro_alto", "")
            attenzione_parts.append(
                f"Il costo del lavoro ({costo_lavoro_pct:.1f}%) supera il range ISA per {label} ({lav_min}–{lav_max}%). "
                + (alert if alert else "")
            )
        elif costo_lavoro_pct < lav_min and costo_lavoro_pct > 0:
            alert = bench.get("alert_costo_lavoro_basso", "")
            attenzione_parts.append(
                f"Il costo del lavoro ({costo_lavoro_pct:.1f}%) è sotto il range ISA per {label} ({lav_min}–{lav_max}%). "
                + (alert if alert else "")
            )

    if bench and bmin and margine_percentuale < bmin:
        alert_m = bench.get("alert_margine_basso", "")
        attenzione_parts.append(alert_m if alert_m else f"Margine sotto la soglia minima ISA ({bmin}%).")

    if not attenzione_parts:
        attenzione_parts.append(
            f"I parametri principali rientrano nei range ISA per {label}. "
            "Nessuna anomalia strutturale rilevata. "
            "Mantenere il monitoraggio trimestrale dei KPI."
        )

    attenzione = " ".join(attenzione_parts)

    # ── Ottimizzazione fiscale ──
    suggestions = []
    has = lambda kw: any(kw in a.get("descrizione", "").lower() for a in (account_breakdown or []))

    if ricavi < FORFETTARIO_SOGLIA and bench.get("forfettario_compatibile", True):
        suggestions.append(
            f"REGIME FORFETTARIO: ricavi di €{ricavi:,.0f} sotto soglia €85.000 e categoria "
            f"({'generalmente compatibile' if bench.get('forfettario_compatibile') else 'da verificare'}). "
            "Valutare concretamente: imposta sostitutiva 15% (5% nei primi 5 anni), esonero IVA. "
            "Verificare cause ostative prima della decisione."
        )

    note_fiscali = bench.get("note_fiscali", "") if bench else ""
    if note_fiscali:
        suggestions.append(f"Note fiscali specifiche {label}: {note_fiscali}")

    if not has("formaz") and not has("aggiorn"):
        suggestions.append("Formazione professionale obbligatoria/ECM: nessuna spesa rilevata — integralmente deducibile.")
    if not has("previd") and not has("pensione"):
        suggestions.append("Previdenza integrativa: deducibile fino a €5.164,57/anno — voce non presente nel periodo.")
    suggestions.append("Verificare deducibilità parziale spese promiscue: telefonia (80%), auto (20%), home office (secondo normativa).")

    ottimizzazione = " ".join(suggestions[:4])

    # ── Raccomandazioni ──
    if bench and bm and margine_percentuale < bm - 5:
        raccomandazioni = (
            f"Priorità: recupero margine verso la media ISA {label} ({bm:.0f}%). "
            "Analizzare le voci di costo con maggiore scostamento dal benchmark. "
            "Impostare un monitoraggio mensile di ricavi e costi variabili nel prossimo trimestre."
        )
    elif bench and bm and margine_percentuale > bm + 5:
        raccomandazioni = (
            f"Performance sopra la media ISA {label}: sfruttare il momento per pianificare investimenti deducibili. "
            "Anticipare acquisti di beni strumentali entro fine anno. "
            "Valutare accantonamenti per previdenza integrativa e formazione."
        )
    else:
        raccomandazioni = (
            f"Posizionamento in linea con i benchmark ISA {label}. "
            "Pianificare gli accantonamenti fiscali di fine periodo. "
            "Confrontare i dati con il periodo precedente per valutare trend di crescita."
        )

    if isa_codice and isa_codice != "n.d.":
        raccomandazioni += f" (Riferimento ISA: {isa_codice})"

    return {
        "analisi_generale":      analisi,
        "punti_attenzione":      attenzione,
        "ottimizzazione_fiscale": ottimizzazione,
        "raccomandazioni":       raccomandazioni,
    }


# ─────────────────────────────────────────────────────────────
# Context builders
# ─────────────────────────────────────────────────────────────

def _build_bench_context(bench, margine_pct, costo_lavoro_pct, costo_autonomi_pct, ricavi):
    if not bench:
        return ""
    lav_min = bench.get("costo_lavoro_min", "n.d.")
    lav_max = bench.get("costo_lavoro_max", "n.d.")
    alert_lav = ""
    if isinstance(lav_min, float) and isinstance(lav_max, float):
        if costo_lavoro_pct > lav_max:
            alert_lav = bench.get("alert_costo_lavoro_alto", "")
        elif 0 < costo_lavoro_pct < lav_min:
            alert_lav = bench.get("alert_costo_lavoro_basso", "")
    alert_m = ""
    bmin = bench.get("margine_pct_min")
    if bmin and margine_pct < bmin:
        alert_m = bench.get("alert_margine_basso", "")

    return (
        f"BENCHMARK ISA 2025/2026 — {bench.get('label','')} (codice {bench.get('isa_codice','n.d.')}):\n"
        f"- Margine % medio: {bench.get('margine_pct_medio')}% (range: {bench.get('margine_pct_min')}–{bench.get('margine_pct_max')}%)\n"
        f"- Costo lavoro / ricavi: {lav_min}–{lav_max}% (cliente: {costo_lavoro_pct:.1f}%)\n"
        f"- Costo collaboratori autonomi / ricavi: {bench.get('costo_autonomo_su_ricavi','')}% (cliente: {costo_autonomi_pct:.1f}%)\n"
        f"- Ricavi medi settore: €{bench.get('ricavi_medi',0):,.0f} (cliente: €{ricavi:,.0f})\n"
        f"- Note settore: {bench.get('note_settore','')}\n"
        f"- Note fiscali: {bench.get('note_fiscali','')}\n"
        f"- Forfettario compatibile: {'Sì' if bench.get('forfettario_compatibile') else 'Raramente / No'}\n"
        + (f"- ALERT COSTO LAVORO: {alert_lav}\n" if alert_lav else "")
        + (f"- ALERT MARGINE: {alert_m}\n" if alert_m else "")
    )


def _build_peer_context(peer_stats):
    if not peer_stats or peer_stats.get("count", 0) < 2:
        return ""
    return (
        f"BENCHMARK PEER (clienti reali stesso settore nel portafoglio, n={peer_stats['count']}):\n"
        f"- Margine % medio peer: {peer_stats['margine_pct_medio']:.1f}%\n"
        f"- Ricavi medi peer: €{peer_stats['ricavi_medi']:,.0f}\n"
        f"- Costi medi peer: €{peer_stats['costi_medi']:,.0f}\n"
        "Confronta esplicitamente il cliente con questi dati reali aggregati e anonimi."
    )


def _build_forfettario_context(ricavi, bench):
    if ricavi >= FORFETTARIO_SOGLIA:
        return ""
    compatibile = bench.get("forfettario_compatibile", True) if bench else True
    return (
        f"ATTENZIONE REGIME FORFETTARIO: ricavi €{ricavi:,.0f} sotto soglia €85.000. "
        f"Categoria {'generalmente compatibile' if compatibile else 'raramente compatibile — verificare cause ostative'}. "
        "Valutare concretamente nella sezione ottimizzazione_fiscale."
    )


# ─────────────────────────────────────────────────────────────
# Calcolo voci da account_breakdown
# ─────────────────────────────────────────────────────────────

def _calc_costo_lavoro_pct(ricavi, accounts):
    if not ricavi:
        return 0.0
    keywords = ["retribuz", "personale", "dipendente", "inps", "apprendist", "tfr"]
    tot = sum(
        a.get("pagamenti", 0) + abs(a.get("rettifiche", 0))
        for a in accounts
        if any(kw in a.get("descrizione", "").lower() for kw in keywords)
    )
    return (tot / ricavi * 100) if tot else 0.0


def _calc_costo_autonomi_pct(ricavi, accounts):
    if not ricavi:
        return 0.0
    keywords = ["lavoro autonomo", "prestaz", "collabor", "consulenz"]
    tot = sum(
        a.get("pagamenti", 0) + abs(a.get("rettifiche", 0))
        for a in accounts
        if any(kw in a.get("descrizione", "").lower() for kw in keywords)
    )
    return (tot / ricavi * 100) if tot else 0.0
