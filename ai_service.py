"""
ai_service.py v4 — Analisi singola con ragguaglio annuale + confronto libero tra periodi.
"""
from __future__ import annotations
import os, json, httpx
from pathlib import Path

FORFETTARIO_SOGLIA = 85000
_BENCH_PATH = Path(__file__).parent / "benchmarks.json"

def _load_benchmarks() -> dict:
    try: return json.loads(_BENCH_PATH.read_text(encoding="utf-8"))
    except: return {}


# ─────────────────────────────────────────────────────────────
# Commento singolo periodo
# ─────────────────────────────────────────────────────────────

def generate_commento(
    client_id: str, periodo: str,
    ricavi: float, costi: float, margine: float, margine_percentuale: float,
    mesi_periodo: int = 12,
    settore: str | None = None,
    account_breakdown: list[dict] | None = None,
    peer_stats: dict | None = None,
    comparison_data: dict | None = None,
) -> str:
    benchmarks = _load_benchmarks()
    bench = benchmarks.get(settore or "altro", benchmarks.get("altro", {}))
    ricavi_ann = ricavi * (12 / mesi_periodo) if mesi_periodo else ricavi
    api_key = os.getenv("OPENAI_API_KEY")
    fn = _openai_single if api_key else _rule_based_single
    return fn(api_key, client_id, periodo, ricavi, costi, margine, margine_percentuale,
              mesi_periodo, ricavi_ann, bench, account_breakdown, peer_stats, comparison_data)


# ─────────────────────────────────────────────────────────────
# Commento confronto due periodi
# ─────────────────────────────────────────────────────────────

def generate_commento_confronto(
    periodo_a: str, parsed_a: dict, mesi_a: int,
    periodo_b: str, parsed_b: dict, mesi_b: int,
    settore: str | None = None,
    bench: dict | None = None,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    bench = bench or {}
    ra_ann = parsed_a["ricavi"] * (12 / mesi_a)
    rb_ann = parsed_b["ricavi"] * (12 / mesi_b)

    if api_key:
        return _openai_confronto(api_key, periodo_a, parsed_a, mesi_a, ra_ann,
                                 periodo_b, parsed_b, mesi_b, rb_ann, settore, bench)
    return _rule_based_confronto(periodo_a, parsed_a, mesi_a, ra_ann,
                                 periodo_b, parsed_b, mesi_b, rb_ann, bench)


# ─────────────────────────────────────────────────────────────
# OpenAI — singolo
# ─────────────────────────────────────────────────────────────

def _openai_single(api_key, client_id, periodo, ricavi, costi, margine, mp,
                   mesi, ricavi_ann, bench, accounts, peer_stats, comp):
    is_q = "-Q" in periodo
    tipo_p = f"trimestrale ({mesi} mesi)" if is_q else "annuale"
    top_costs = _top_costs(accounts)
    cl_pct = _calc_costo_lavoro_pct(ricavi, accounts or [])
    ca_pct = _calc_costo_autonomi_pct(ricavi, accounts or [])

    bench_ctx = _build_bench_ctx(bench, mp, cl_pct, ca_pct, ricavi)
    peer_ctx  = _build_peer_ctx(peer_stats)
    trend_ctx = _build_trend_ctx(comp, periodo)

    forf_soglia = ricavi_ann < FORFETTARIO_SOGLIA
    forf_nota = ""
    if is_q:
        forf_nota = (
            f"ATTENZIONE: il dato è TRIMESTRALE ({mesi} mesi). "
            f"I ricavi di €{ricavi:,.0f} annualizzati diventano €{ricavi_ann:,.0f}. "
            f"La soglia forfettario (€85.000) va valutata sul dato ANNUALIZZATO. "
            + ("Con €{:,.0f} annualizzati il forfettario potrebbe essere applicabile — verificare.".format(ricavi_ann) if forf_soglia
               else f"Con €{ricavi_ann:,.0f} annualizzati la soglia forfettario è SUPERATA: non menzionare il forfettario come opzione praticabile.")
        )
    elif ricavi < FORFETTARIO_SOGLIA and bench.get("forfettario_compatibile", True):
        forf_nota = (f"FORFETTARIO: ricavi annuali €{ricavi:,.0f} sotto €85.000 — "
                     "imposta sostitutiva 15% (5% primi 5 anni), esonero IVA. Verificare cause ostative.")

    payload = {"periodo": periodo, "tipo_periodo": tipo_p,
               "ricavi": ricavi, "ricavi_annualizzati": round(ricavi_ann, 0),
               "costi": costi, "margine": margine, "margine_percentuale": mp,
               "costo_lavoro_pct": round(cl_pct,1),
               "costo_collaboratori_autonomi_pct": round(ca_pct,1),
               "top_voci_di_costo": top_costs}

    prompt = f"""Sei un commercialista esperto italiano. Analizza i dati finanziari {tipo_p} e produci un commento in 4 sezioni.

{bench_ctx}{peer_ctx}{trend_ctx}
{forf_nota}

Dati:
{json.dumps(payload, indent=2, ensure_ascii=False)}

REGOLE IMPORTANTI:
- Se il dato è trimestrale, tutte le considerazioni su soglie annuali (forfettario, volume d'affari) devono usare il dato annualizzato, NON quello trimestrale.
- Non menzionare il forfettario come opzione se i ricavi annualizzati superano €85.000.
- Confronta con benchmark ISA e peer se disponibili.
- Max 4 frasi per sezione. Sii diretto e professionale.

JSON (solo questo, nessun testo fuori):
{{"analisi_generale":"...","punti_attenzione":"...","ottimizzazione_fiscale":"...","raccomandazioni":"..."}}"""

    return _call_openai(api_key, prompt) or _rule_based_single(
        None, client_id, periodo, ricavi, costi, margine, mp,
        mesi, ricavi_ann, bench, accounts, peer_stats, comp)


# ─────────────────────────────────────────────────────────────
# OpenAI — confronto
# ─────────────────────────────────────────────────────────────

def _openai_confronto(api_key, pa, da, ma, ra_ann, pb, db_, mb, rb_ann, settore, bench):
    label = bench.get("label", settore or "settore")
    tipo_a = f"trimestrale ({ma} mesi)" if "-Q" in pa else "annuale"
    tipo_b = f"trimestrale ({mb} mesi)" if "-Q" in pb else "annuale"

    # Normalizza a valori mensili per confronto equo se durate diverse
    same_duration = (ma == mb)
    norm_note = ""
    if not same_duration:
        norm_note = (
            f"NOTA: i due periodi hanno durate diverse ({ma} vs {mb} mesi). "
            "Per un confronto equo usa i valori mensili medi (ricavi/mesi, costi/mesi). "
            "Evidenzia questa differenza nell'analisi."
        )

    delta_r  = ((da["ricavi"]  - db_["ricavi"])  / db_["ricavi"]  * 100) if db_["ricavi"]  else 0
    delta_mp = da["margine_percentuale"] - db_["margine_percentuale"]

    # Annualized forfettario check
    forf_a = ra_ann < FORFETTARIO_SOGLIA and bench.get("forfettario_compatibile", True)
    forf_b = rb_ann < FORFETTARIO_SOGLIA and bench.get("forfettario_compatibile", True)
    forf_nota = ""
    if forf_a or forf_b:
        forf_nota = (f"FORFETTARIO: {"entrambi i periodi hanno" if forf_a and forf_b else "uno dei periodi ha"} "
                     f"ricavi annualizzati sotto €85.000 (A: €{ra_ann:,.0f}, B: €{rb_ann:,.0f}). "
                     "Valutare nella sezione ottimizzazione_fiscale.")

    payload = {
        "periodo_A": {"periodo": pa, "tipo": tipo_a,
                      "ricavi": da["ricavi"], "ricavi_annualizzati": round(ra_ann),
                      "costi": da["costi"], "margine": da["margine"],
                      "margine_percentuale": da["margine_percentuale"]},
        "periodo_B": {"periodo": pb, "tipo": tipo_b,
                      "ricavi": db_["ricavi"], "ricavi_annualizzati": round(rb_ann),
                      "costi": db_["costi"], "margine": db_["margine"],
                      "margine_percentuale": db_["margine_percentuale"]},
        "delta_ricavi_A_vs_B_pct": round(delta_r, 1),
        "delta_margine_pp": round(delta_mp, 1),
        "settore": label,
        "benchmark_margine_medio": bench.get("margine_pct_medio"),
    }

    prompt = f"""Sei un commercialista esperto italiano. Confronta i due periodi finanziari e produci un'analisi comparativa in 4 sezioni.

Benchmark ISA {label}: margine medio {bench.get("margine_pct_medio")}% (range {bench.get("margine_pct_min")}–{bench.get("margine_pct_max")}%).
{norm_note}
{forf_nota}

Dati confronto:
{json.dumps(payload, indent=2, ensure_ascii=False)}

SEZIONI RICHIESTE:
1. sintesi_confronto: variazioni principali tra A e B (ricavi, costi, margine), con riferimento al benchmark ISA
2. analisi_variazioni: voci che hanno guidato il cambiamento, spiegazione delle cause probabili
3. ottimizzazione_fiscale: considerazioni fiscali specifiche per entrambi i periodi (usa ricavi ANNUALIZZATI per il forfettario)
4. raccomandazioni: azioni prioritarie per migliorare il periodo successivo

Max 4 frasi per sezione. Non inventare dati. Non citare il cliente per nome.

JSON (solo questo):
{{"sintesi_confronto":"...","analisi_variazioni":"...","ottimizzazione_fiscale":"...","raccomandazioni":"..."}}"""

    return _call_openai(api_key, prompt) or _rule_based_confronto(
        pa, da, ma, ra_ann, pb, db_, mb, rb_ann, bench)


# ─────────────────────────────────────────────────────────────
# Rule-based — singolo
# ─────────────────────────────────────────────────────────────

def _rule_based_single(api_key, client_id, periodo, ricavi, costi, margine, mp,
                       mesi, ricavi_ann, bench, accounts, peer_stats, comp):
    is_q = "-Q" in periodo
    tipo = f"trimestrale ({mesi} mesi)" if is_q else "annuale"
    cost_r = (costi/ricavi*100) if ricavi else 0
    label  = bench.get("label","settore") if bench else "settore"
    bm     = bench.get("margine_pct_medio") if bench else None
    bmin   = bench.get("margine_pct_min") if bench else None
    bmax   = bench.get("margine_pct_max") if bench else None
    lm,lM  = bench.get("costo_lavoro_min") if bench else None, bench.get("costo_lavoro_max") if bench else None
    isa    = bench.get("isa_codice","") if bench else ""
    cl_pct = _calc_costo_lavoro_pct(ricavi, accounts or [])

    # Analisi
    if bm:
        diff = mp - bm
        vs = (f"superiore di {diff:.1f}pp alla media ISA {label} ({bm:.0f}%)" if diff>5 else
              f"inferiore di {abs(diff):.1f}pp alla media ISA {label} ({bm:.0f}%)" if diff<-5 else
              f"in linea con la media ISA {label} ({bm:.0f}%)")
    else: vs = "nella media del settore"

    ann_note = f" (dato {tipo}; ricavi annualizzati: €{ricavi_ann:,.0f})" if is_q else ""
    analisi = (f"Il periodo {periodo}{ann_note} evidenzia ricavi di €{ricavi:,.0f} e margine "
               f"del {mp:.1f}%, {vs}. Costi al {cost_r:.1f}% dei ricavi.")
    if comp:
        dr = comp["delta_ricavi_pct"]; dm = comp["delta_margine_pp"]
        analisi += (f" Vs {comp['periodo_precedente']}: ricavi "
                    f"{'+' if dr>=0 else ''}{dr:.1f}%, margine {'+' if dm>=0 else ''}{dm:.1f}pp.")
    if peer_stats and peer_stats.get("count",0)>=2:
        pd = mp - peer_stats["margine_pct_medio"]
        analisi += f" Vs {peer_stats['count']} peer: margine {'+' if pd>=0 else ''}{pd:.1f}pp."

    # Attenzione
    att = []
    if lm and lM and cl_pct > lM:
        att.append(bench.get("alert_costo_lavoro_alto","") or f"Costo lavoro ({cl_pct:.1f}%) sopra range ISA.")
    if bmin and mp < bmin:
        att.append(bench.get("alert_margine_basso","") or f"Margine sotto soglia ISA ({bmin}%).")
    if comp and comp["delta_ricavi_pct"] < -10:
        att.append(f"Ricavi calati del {abs(comp['delta_ricavi_pct']):.1f}% vs {comp['periodo_precedente']}.")
    attenzione = " ".join(att) if att else f"Nessuna anomalia rispetto ai range ISA per {label}."

    # Fiscale
    sugg = []
    has = lambda kw: any(kw in a.get("descrizione","").lower() for a in (accounts or []))
    # FORFETTARIO — usa sempre ricavi annualizzati
    forf_applicable = ricavi_ann < FORFETTARIO_SOGLIA and bench.get("forfettario_compatibile", True)
    if is_q:
        if forf_applicable:
            sugg.append(f"FORFETTARIO: ricavi annualizzati €{ricavi_ann:,.0f} < €85.000 — potenzialmente applicabile. Verificare su base annuale completa.")
        else:
            sugg.append(f"Dato trimestrale: ricavi annualizzati €{ricavi_ann:,.0f} — soglia forfettario superata su base annua.")
    elif forf_applicable:
        sugg.append(f"FORFETTARIO: ricavi €{ricavi:,.0f} < €85.000 — imposta sostitutiva 15%, esonero IVA.")
    if bench and bench.get("note_fiscali"):
        sugg.append(f"Note {label}: {bench['note_fiscali']}")
    if not has("formaz"): sugg.append("Formazione professionale: integralmente deducibile, voce assente.")
    if not has("previd"): sugg.append("Previdenza integrativa: deducibile fino a €5.164,57/anno.")
    ottimizzazione = " ".join(sugg[:3])

    # Raccomandazioni
    if bm and mp < bm-5:
        racc = f"Priorità: recupero margine verso media ISA {label} ({bm:.0f}%). Analisi costi nel trimestre successivo."
    elif is_q:
        racc = f"Monitorare l'andamento per proiettare il risultato annuale. Ricavi annualizzati: €{ricavi_ann:,.0f}."
    else:
        racc = f"Posizionamento {'positivo' if mp>=bm else 'da migliorare'} rispetto al benchmark ISA. Pianificare accantonamenti fiscali."
    if isa and isa != "n.d.": racc += f" (ISA: {isa})"

    return json.dumps({"analisi_generale": analisi, "punti_attenzione": attenzione,
                       "ottimizzazione_fiscale": ottimizzazione, "raccomandazioni": racc},
                      ensure_ascii=False)


# ─────────────────────────────────────────────────────────────
# Rule-based — confronto
# ─────────────────────────────────────────────────────────────

def _rule_based_confronto(pa, da, ma, ra_ann, pb, db_, mb, rb_ann, bench):
    label = bench.get("label","settore") if bench else "settore"
    bm    = bench.get("margine_pct_medio") if bench else None
    is_qa, is_qb = "-Q" in pa, "-Q" in pb
    same_dur = (ma == mb)

    # Delta
    dr = ((da["ricavi"] - db_["ricavi"]) / db_["ricavi"] * 100) if db_["ricavi"] else 0
    dc = ((da["costi"]  - db_["costi"])  / db_["costi"]  * 100) if db_["costi"]  else 0
    dm = da["margine_percentuale"] - db_["margine_percentuale"]

    # Sintesi
    note_dur = f" (periodi di durata diversa: {ma} vs {mb} mesi — confronto su valori mensili)" if not same_dur else ""
    sintesi = (f"Confronto {pa} vs {pb}{note_dur}: "
               f"i ricavi passano da €{db_['ricavi']:,.0f} a €{da['ricavi']:,.0f} "
               f"({'+' if dr>=0 else ''}{dr:.1f}%), il margine "
               f"{'migliora' if dm>=0 else 'peggiora'} di {abs(dm):.1f}pp "
               f"({db_['margine_percentuale']:.1f}% → {da['margine_percentuale']:.1f}%).")
    if bm:
        m_vs = "entrambi sopra" if da["margine_percentuale"]>bm and db_["margine_percentuale"]>bm else \
               "entrambi sotto" if da["margine_percentuale"]<bm and db_["margine_percentuale"]<bm else \
               "con andamento misto rispetto a"
        sintesi += f" I margini sono {m_vs} la media ISA {label} ({bm:.0f}%)."

    # Variazioni
    var = (f"I costi nel periodo {pa} sono {'aumentati' if dc>0 else 'diminuiti'} "
           f"del {abs(dc):.1f}% rispetto a {pb} "
           f"(€{db_['costi']:,.0f} → €{da['costi']:,.0f}). "
           f"Il margine operativo {'è migliorato' if dm>=0 else 'ha subito una contrazione'} "
           f"di {abs(dm):.1f} punti percentuali.")

    # Fiscale
    forf_a = ra_ann < FORFETTARIO_SOGLIA and bench.get("forfettario_compatibile", True)
    forf_b = rb_ann < FORFETTARIO_SOGLIA and bench.get("forfettario_compatibile", True)
    fisc = ""
    if forf_a or forf_b:
        fisc = (f"Ricavi annualizzati: {pa}=€{ra_ann:,.0f}, {pb}=€{rb_ann:,.0f}. "
                + ("Entrambi i periodi sotto soglia forfettario €85.000 su base annua. " if forf_a and forf_b
                   else f"Il periodo {'A' if forf_a else 'B'} è sotto soglia forfettario annualizzata. "))
    fisc += "Verificare la deducibilità di formazione, previdenza integrativa e spese strumentali."

    # Raccomandazioni
    if dm < -3:
        racc = (f"Il calo del margine di {abs(dm):.1f}pp richiede un'analisi delle voci di costo in crescita. "
                "Impostare target di recupero margine per i prossimi periodi.")
    else:
        racc = (f"Tendenza {'positiva' if dm>=0 else 'da monitorare'}. "
                "Continuare il monitoraggio trimestrale e confrontare con il benchmark ISA.")

    return json.dumps({"sintesi_confronto": sintesi, "analisi_variazioni": var,
                       "ottimizzazione_fiscale": fisc, "raccomandazioni": racc},
                      ensure_ascii=False)


# ─────────────────────────────────────────────────────────────
# Context builders & utils
# ─────────────────────────────────────────────────────────────

def _build_bench_ctx(bench, mp, cl, ca, ricavi):
    if not bench: return ""
    lm,lM = bench.get("costo_lavoro_min","n.d."), bench.get("costo_lavoro_max","n.d.")
    al = bench.get("alert_costo_lavoro_alto","") if isinstance(lm,float) and cl>lM else ""
    am = bench.get("alert_margine_basso","") if bench.get("margine_pct_min") and mp<bench["margine_pct_min"] else ""
    return (f"BENCHMARK ISA {bench.get('label','')} (cod. {bench.get('isa_codice','n.d.')}):\n"
            f"- Margine: {bench.get('margine_pct_medio')}% (range {bench.get('margine_pct_min')}–{bench.get('margine_pct_max')}%)\n"
            f"- Costo lavoro: {lm}–{lM}% (cliente: {cl:.1f}%) | Autonomi: {bench.get('costo_autonomo_su_ricavi','')}% (cliente: {ca:.1f}%)\n"
            f"- Note: {bench.get('note_settore','')}\n"
            f"- Note fiscali: {bench.get('note_fiscali','')}\n"
            f"- Forfettario: {'Sì' if bench.get('forfettario_compatibile') else 'No/Raramente'}\n"
            + (f"- ALERT LAVORO: {al}\n" if al else "")
            + (f"- ALERT MARGINE: {am}\n" if am else ""))

def _build_peer_ctx(p):
    if not p or p.get("count",0)<2: return ""
    return (f"PEER (n={p['count']}): margine medio {p['margine_pct_medio']:.1f}%, "
            f"ricavi medi €{p['ricavi_medi']:,.0f}\n")

def _build_trend_ctx(c, periodo):
    if not c: return ""
    return (f"TREND vs {c['periodo_precedente']}: "
            f"Δricavi {c['delta_ricavi_pct']:+.1f}%, "
            f"Δmargine {c['delta_margine_pp']:+.1f}pp\n")

def _top_costs(accounts):
    if not accounts: return []
    costs = sorted([a for a in accounts if a.get("tipo")=="costo"],
                   key=lambda x: x.get("pagamenti",0)+abs(x.get("rettifiche",0)), reverse=True)
    return [{"descrizione": c["descrizione"], "importo": c["pagamenti"]} for c in costs[:6]]

def _calc_costo_lavoro_pct(ricavi, accs):
    if not ricavi: return 0.
    kw = ["retribuz","personale","dipendente","inps","apprendist","tfr"]
    t = sum(a.get("pagamenti",0)+abs(a.get("rettifiche",0)) for a in accs
            if any(k in a.get("descrizione","").lower() for k in kw))
    return (t/ricavi*100) if t else 0.

def _calc_costo_autonomi_pct(ricavi, accs):
    if not ricavi: return 0.
    kw = ["lavoro autonomo","prestaz","collabor","consulenz"]
    t = sum(a.get("pagamenti",0)+abs(a.get("rettifiche",0)) for a in accs
            if any(k in a.get("descrizione","").lower() for k in kw))
    return (t/ricavi*100) if t else 0.

def _call_openai(api_key, prompt):
    try:
        r = httpx.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model":"gpt-4o-mini","messages":[{"role":"user","content":prompt}],
                  "max_tokens":1000,"temperature":0.35},
            timeout=35)
        r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"].strip()
        raw = raw.replace("```json","").replace("```","").strip()
        return json.dumps(json.loads(raw), ensure_ascii=False)
    except Exception:
        return None
