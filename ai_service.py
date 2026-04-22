"""
ai_service.py — Commento AI con benchmark ISA, peer e trend trimestrale.
"""
from __future__ import annotations
import os, json, httpx
from pathlib import Path

FORFETTARIO_SOGLIA = 85000
_BENCH_PATH = Path(__file__).parent / "benchmarks.json"

def _load_benchmarks() -> dict:
    try: return json.loads(_BENCH_PATH.read_text(encoding="utf-8"))
    except Exception: return {}

def generate_commento(
    client_id: str, periodo: str,
    ricavi: float, costi: float, margine: float, margine_percentuale: float,
    settore: str | None = None,
    account_breakdown: list[dict] | None = None,
    peer_stats: dict | None = None,
    comparison_data: dict | None = None,   # ← NUOVO
) -> str:
    benchmarks = _load_benchmarks()
    bench = benchmarks.get(settore or "altro", benchmarks.get("altro", {}))
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        return _openai_comment(api_key, client_id, periodo, ricavi, costi, margine,
                               margine_percentuale, bench, account_breakdown,
                               peer_stats, comparison_data)
    return _rule_based_comment(ricavi, costi, margine, margine_percentuale,
                               bench, account_breakdown, peer_stats, comparison_data)


# ─────────────────────────────────────────────────────────────
# OpenAI
# ─────────────────────────────────────────────────────────────

def _openai_comment(api_key, client_id, periodo, ricavi, costi, margine,
                    margine_percentuale, bench, account_breakdown, peer_stats, comparison_data):
    top_costs = []
    if account_breakdown:
        costs = sorted([a for a in account_breakdown if a.get("tipo") == "costo"],
                       key=lambda x: x.get("pagamenti",0)+abs(x.get("rettifiche",0)), reverse=True)
        top_costs = [{"descrizione": c["descrizione"], "importo": c["pagamenti"]} for c in costs[:8]]

    costo_lavoro_pct   = _calc_costo_lavoro_pct(ricavi, account_breakdown or [])
    costo_autonomi_pct = _calc_costo_autonomi_pct(ricavi, account_breakdown or [])

    bench_ctx = _build_bench_context(bench, margine_percentuale, costo_lavoro_pct, costo_autonomi_pct, ricavi)
    peer_ctx  = _build_peer_context(peer_stats)
    forf_ctx  = _build_forfettario_context(ricavi, bench)
    trend_ctx = _build_trend_context(comparison_data, periodo)

    is_quarterly = "-Q" in periodo
    granularity  = "trimestrale" if is_quarterly else "annuale"

    payload = {
        "periodo": periodo,
        "tipo_periodo": granularity,
        "ricavi": ricavi, "costi": costi,
        "margine": margine, "margine_percentuale_sui_ricavi": margine_percentuale,
        "costo_lavoro_pct": round(costo_lavoro_pct, 1),
        "costo_collaboratori_autonomi_pct": round(costo_autonomi_pct, 1),
        "top_voci_di_costo": top_costs,
    }

    prompt = f"""Sei un commercialista esperto italiano. Analizza i dati finanziari {granularity}i e produci un commento in QUATTRO sezioni.

{bench_ctx}
{peer_ctx}
{trend_ctx}
{forf_ctx}

Dati del cliente ({granularity}):
{json.dumps(payload, indent=2, ensure_ascii=False)}

ISTRUZIONI:
1. analisi_generale: confronta con benchmark ISA e peer. Se sono disponibili dati del periodo precedente, commenta l'andamento (crescita/calo ricavi, miglioramento/peggioramento margine). Per dati trimestrali, contestualizza la stagionalità tipica del settore.
2. punti_attenzione: alert su costo lavoro fuori range, voci anomale, trend negativi rispetto al periodo precedente.
3. ottimizzazione_fiscale: suggerimenti specifici per il settore, forfettario se applicabile, opportunità di fine periodo/fine anno.
4. raccomandazioni: azioni prioritarie con orizzonte temporale chiaro (entro fine trimestre / fine anno).

Max 5 frasi per sezione. Non inventare dati. Non citare il cliente per nome.

Rispondi SOLO con JSON valido:
{{"analisi_generale":"...","punti_attenzione":"...","ottimizzazione_fiscale":"...","raccomandazioni":"..."}}"""

    try:
        response = httpx.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 1000, "temperature": 0.35},
            timeout=35,
        )
        response.raise_for_status()
        raw = response.json()["choices"][0]["message"]["content"].strip()
        raw = raw.replace("```json","").replace("```","").strip()
        return json.dumps(json.loads(raw), ensure_ascii=False)
    except Exception:
        return _rule_based_comment(ricavi, costi, margine, margine_percentuale,
                                   bench, account_breakdown, peer_stats, comparison_data)


# ─────────────────────────────────────────────────────────────
# Rule-based fallback
# ─────────────────────────────────────────────────────────────

def _rule_based_comment(ricavi, costi, margine, margine_percentuale,
                        bench, account_breakdown, peer_stats, comparison_data):
    return json.dumps(
        _rule_based_structured(ricavi, costi, margine, margine_percentuale,
                               bench, account_breakdown, peer_stats, comparison_data),
        ensure_ascii=False)

def _rule_based_structured(ricavi, costi, margine, margine_percentuale,
                            bench, account_breakdown, peer_stats, comparison_data):
    cost_ratio         = (costi / ricavi * 100) if ricavi else 0
    costo_lavoro_pct   = _calc_costo_lavoro_pct(ricavi, account_breakdown or [])
    costo_autonomi_pct = _calc_costo_autonomi_pct(ricavi, account_breakdown or [])

    label   = bench.get("label", "settore") if bench else "settore"
    bm      = bench.get("margine_pct_medio") if bench else None
    bmin    = bench.get("margine_pct_min") if bench else None
    bmax    = bench.get("margine_pct_max") if bench else None
    lav_min = bench.get("costo_lavoro_min") if bench else None
    lav_max = bench.get("costo_lavoro_max") if bench else None
    isa     = bench.get("isa_codice", "") if bench else ""

    # ── Analisi generale ──
    if bm:
        diff = margine_percentuale - bm
        vs_bench = (f"superiore di {diff:.1f}pp alla media ISA {label} ({bm:.0f}%)" if diff > 5 else
                    f"inferiore di {abs(diff):.1f}pp alla media ISA {label} ({bm:.0f}%)" if diff < -5 else
                    f"in linea con la media ISA {label} ({bm:.0f}%)")
    else:
        vs_bench = "nella media del settore"

    analisi = (f"Il periodo evidenzia ricavi di €{ricavi:,.0f} e un margine del "
               f"{margine_percentuale:.1f}%, {vs_bench}. "
               f"L'incidenza dei costi sui ricavi è del {cost_ratio:.1f}%.")

    # Trend vs periodo precedente
    if comparison_data:
        dr = comparison_data["delta_ricavi_pct"]
        dm = comparison_data["delta_margine_pp"]
        prev = comparison_data["periodo_precedente"]
        analisi += (
            f" Rispetto a {prev}, i ricavi sono {'cresciuti' if dr>=0 else 'calati'} "
            f"del {abs(dr):.1f}% e il margine è {'migliorato' if dm>=0 else 'peggiorato'} "
            f"di {abs(dm):.1f} punti percentuali."
        )

    if peer_stats and peer_stats.get("count", 0) >= 2:
        pdiff = margine_percentuale - peer_stats["margine_pct_medio"]
        analisi += (f" Vs {peer_stats['count']} peer reali: margine "
                    f"{'superiore' if pdiff>=0 else 'inferiore'} di {abs(pdiff):.1f}pp "
                    f"(media peer {peer_stats['margine_pct_medio']:.1f}%).")

    # ── Punti di attenzione ──
    attenzione_parts = []
    if lav_min and lav_max and costo_lavoro_pct > lav_max:
        attenzione_parts.append(bench.get("alert_costo_lavoro_alto","") or
            f"Costo lavoro ({costo_lavoro_pct:.1f}%) sopra il range ISA {label} ({lav_min}–{lav_max}%).")
    elif lav_min and costo_lavoro_pct > 0 and costo_lavoro_pct < lav_min:
        attenzione_parts.append(bench.get("alert_costo_lavoro_basso","") or
            f"Costo lavoro ({costo_lavoro_pct:.1f}%) sotto il range ISA {label} ({lav_min}–{lav_max}%).")
    if bmin and margine_percentuale < bmin:
        attenzione_parts.append(bench.get("alert_margine_basso","") or
            f"Margine sotto la soglia minima ISA {label} ({bmin}%).")
    if comparison_data and comparison_data["delta_ricavi_pct"] < -10:
        attenzione_parts.append(
            f"I ricavi sono calati del {abs(comparison_data['delta_ricavi_pct']):.1f}% "
            f"rispetto a {comparison_data['periodo_precedente']}: monitorare attentamente.")
    if not attenzione_parts:
        attenzione_parts.append(
            f"I parametri rientrano nei range ISA per {label}. Nessuna anomalia strutturale.")

    attenzione = " ".join(attenzione_parts)

    # ── Ottimizzazione fiscale ──
    suggestions = []
    has = lambda kw: any(kw in a.get("descrizione","").lower() for a in (account_breakdown or []))

    if ricavi < FORFETTARIO_SOGLIA and bench.get("forfettario_compatibile", True):
        suggestions.append(
            f"FORFETTARIO: ricavi €{ricavi:,.0f} sotto €85.000 — "
            "aliquota 15% (5% primi 5 anni), esonero IVA. Verificare cause ostative.")
    if bench and bench.get("note_fiscali",""):
        suggestions.append(f"Note {label}: {bench['note_fiscali']}")
    if not has("formaz") and not has("aggiorn"):
        suggestions.append("Formazione professionale: nessuna spesa — integralmente deducibile.")
    if not has("previd") and not has("pensione"):
        suggestions.append("Previdenza integrativa: deducibile fino a €5.164,57/anno — voce assente.")
    suggestions.append("Verificare deducibilità spese promiscue: telefonia 80%, auto 20%.")

    ottimizzazione = " ".join(suggestions[:4])

    # ── Raccomandazioni ──
    if comparison_data and comparison_data["delta_margine_pp"] < -5:
        raccomandazioni = (
            f"Il calo del margine di {abs(comparison_data['delta_margine_pp']):.1f}pp "
            f"rispetto a {comparison_data['periodo_precedente']} richiede un'analisi delle voci di costo in crescita. "
            "Impostare un monitoraggio mensile e valutare azioni correttive entro il trimestre successivo.")
    elif bm and margine_percentuale < bm - 5:
        raccomandazioni = (
            f"Margine sotto la media ISA {label} ({bm:.0f}%): analizzare le voci di costo principali "
            "e impostare target di miglioramento per il prossimo periodo.")
    else:
        raccomandazioni = (
            f"Posizionamento in linea con i benchmark ISA {label}. "
            "Pianificare accantonamenti fiscali e valutare investimenti deducibili "
            "entro la chiusura del periodo.")
    if isa and isa != "n.d.":
        raccomandazioni += f" (ISA: {isa})"

    return {"analisi_generale": analisi, "punti_attenzione": attenzione,
            "ottimizzazione_fiscale": ottimizzazione, "raccomandazioni": raccomandazioni}


# ─────────────────────────────────────────────────────────────
# Context builders
# ─────────────────────────────────────────────────────────────

def _build_bench_context(bench, mp, cl_pct, ca_pct, ricavi):
    if not bench: return ""
    lm, lM = bench.get("costo_lavoro_min","n.d."), bench.get("costo_lavoro_max","n.d.")
    alert_lav = ""
    if isinstance(lm, float) and isinstance(lM, float):
        if cl_pct > lM: alert_lav = bench.get("alert_costo_lavoro_alto","")
        elif 0 < cl_pct < lm: alert_lav = bench.get("alert_costo_lavoro_basso","")
    alert_m = bench.get("alert_margine_basso","") if bench.get("margine_pct_min") and mp < bench["margine_pct_min"] else ""
    return (
        f"BENCHMARK ISA 2025/2026 — {bench.get('label','')} (cod. {bench.get('isa_codice','n.d.')}):\n"
        f"- Margine medio: {bench.get('margine_pct_medio')}% (range: {bench.get('margine_pct_min')}–{bench.get('margine_pct_max')}%)\n"
        f"- Costo lavoro: {lm}–{lM}% (cliente: {cl_pct:.1f}%)\n"
        f"- Collaboratori autonomi: {bench.get('costo_autonomo_su_ricavi','')}% (cliente: {ca_pct:.1f}%)\n"
        f"- Ricavi medi settore: €{bench.get('ricavi_medi',0):,.0f} (cliente: €{ricavi:,.0f})\n"
        f"- Note settore: {bench.get('note_settore','')}\n"
        f"- Note fiscali: {bench.get('note_fiscali','')}\n"
        f"- Forfettario: {'Sì' if bench.get('forfettario_compatibile') else 'Raramente/No'}\n"
        + (f"- ALERT LAVORO: {alert_lav}\n" if alert_lav else "")
        + (f"- ALERT MARGINE: {alert_m}\n" if alert_m else "")
    )

def _build_peer_context(peer_stats):
    if not peer_stats or peer_stats.get("count",0) < 2: return ""
    return (
        f"PEER REALI (n={peer_stats['count']}, stesso settore, dati anonimi):\n"
        f"- Margine % medio: {peer_stats['margine_pct_medio']:.1f}%\n"
        f"- Ricavi medi: €{peer_stats['ricavi_medi']:,.0f}\n"
        f"- Costi medi: €{peer_stats['costi_medi']:,.0f}\n"
    )

def _build_trend_context(comp, periodo):
    if not comp: return ""
    is_q = "-Q" in periodo
    label_prec = f"trimestre precedente ({comp['periodo_precedente']})" if is_q else f"anno precedente ({comp['periodo_precedente']})"
    return (
        f"CONFRONTO CON PERIODO PRECEDENTE ({label_prec}):\n"
        f"- Δ Ricavi: {comp['delta_ricavi_pct']:+.1f}%  (€{comp['ricavi_prec']:,.0f} → corrente)\n"
        f"- Δ Costi:  {comp['delta_costi_pct']:+.1f}%  (€{comp['costi_prec']:,.0f} → corrente)\n"
        f"- Δ Margine: {comp['delta_margine_pp']:+.1f}pp  ({comp['margine_pct_prec']:.1f}% → corrente)\n"
        "Commenta esplicitamente questi delta nella sezione analisi_generale e raccomandazioni.\n"
    )

def _build_forfettario_context(ricavi, bench):
    if ricavi >= FORFETTARIO_SOGLIA: return ""
    comp = bench.get("forfettario_compatibile", True) if bench else True
    return (
        f"ATTENZIONE FORFETTARIO: ricavi €{ricavi:,.0f} sotto €85.000. "
        f"Categoria {'compatibile' if comp else 'raramente compatibile — verificare'}. "
        "Valutare nella sezione ottimizzazione_fiscale.\n"
    )

def _calc_costo_lavoro_pct(ricavi, accounts):
    if not ricavi: return 0.0
    kw = ["retribuz","personale","dipendente","inps","apprendist","tfr"]
    tot = sum(a.get("pagamenti",0)+abs(a.get("rettifiche",0)) for a in accounts
              if any(k in a.get("descrizione","").lower() for k in kw))
    return (tot / ricavi * 100) if tot else 0.0

def _calc_costo_autonomi_pct(ricavi, accounts):
    if not ricavi: return 0.0
    kw = ["lavoro autonomo","prestaz","collabor","consulenz"]
    tot = sum(a.get("pagamenti",0)+abs(a.get("rettifiche",0)) for a in accounts
              if any(k in a.get("descrizione","").lower() for k in kw))
    return (tot / ricavi * 100) if tot else 0.0
