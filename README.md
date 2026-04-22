# Accounting Analytics Platform

Piattaforma per l'analisi automatica di Prospetti Reddito Excel esportati da gestionali contabili.

## Struttura

```
platform/
├── backend/
│   ├── main.py          # FastAPI app — tutti gli endpoint REST
│   ├── parser.py        # Parser Excel robusto (openpyxl + pandas)
│   ├── database.py      # SQLAlchemy models (SQLite)
│   ├── ai_service.py    # Commento AI (OpenAI / fallback rule-based)
│   └── requirements.txt
└── frontend/
    └── index.html       # Dashboard standalone (zero dipendenze)
```

## Avvio rapido (backend)

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

## Variabili d'ambiente

```bash
OPENAI_API_KEY=sk-...   # opzionale — usa commento rule-based se assente
```

## Endpoint API

| Metodo | Path | Descrizione |
|--------|------|-------------|
| `POST` | `/upload` | Carica file Excel, estrae KPI + commento AI |
| `GET`  | `/reports` | Lista tutti i report |
| `GET`  | `/reports/{id}` | Report completo con dettaglio conti |
| `GET`  | `/clients` | Lista clienti |
| `GET`  | `/clients/{id}/reports` | Report di un cliente |
| `DELETE` | `/reports/{id}` | Elimina un report |

## Risposta `/upload`

```json
{
  "id": 1,
  "client_id": "401",
  "periodo": "2025",
  "ricavi": 975533.59,
  "costi": 442012.70,
  "margine": 533520.89,
  "margine_percentuale": 54.69,
  "commento_ai": "Il periodo evidenzia ricavi...",
  "accounts": [...]
}
```

## Parser — logica di estrazione

Il parser `parser.py` è progettato per il template specifico del gestionale:

- **Colonne rilevate automaticamente** tramite alias normalizzati
- **Riga header** trovata dinamicamente (non si assume sia sempre la riga 1)
- **Gerarchia** — solo i conti top-level (nessuna indentazione) contribuiscono ai totali
- **Ricavi** = righe con `Incassi > 0`
- **Costi** = righe con `Pagamenti > 0` oppure `Rettifiche > 0` (per ammortamenti)
- **Client ID** estratto dal nome file (es: `401_Prospetto...xlsx` → `401`)
- **Anno/periodo** estratto dal nome file (es: `...2025.xlsx` → `2025`)

## Privacy

- Il file Excel NON viene salvato
- Solo dati strutturati vengono persistiti nel database
- Il cliente è identificato SOLO dal codice numerico

## Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY backend/ .
RUN pip install -r requirements.txt
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```
