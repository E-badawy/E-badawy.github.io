# CIGMA Data Profiler

A full-stack data quality assessment app. Upload a dataset and get:
- Data type (quantitative/qualitative)
- File nature (csv, xlsx, tsv, docx, json, xml)
- Rows/columns and data grain
- Missing values
- Numeric distributions
- Suggestions for feasible analysis
- Anomaly flags and histograms
- Exportable CSV/PDF report
- File preview, outlier summary, and distribution labels
- Analysis fit guidance
- Sample dataset button for instant testing
- Transformation tools for duplicates and missing values
- Async AI insights jobs and server metrics
- Data quality score, root cause & blast radius, and auto-remediation plan

## Run locally

### 1) Backend

```powershell
cd "c:\Users\hp\Desktop\IBM\badawy dqa\backend"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app:app --reload
```

### 1b) Enable AI insights (OpenAI)

Option A: Use a `.env` file

Create a `.env` file in `badawy dqa` (same level as `backend/`) with:

```text
OPENAI_API_KEY=YOUR_KEY_HERE
OPENAI_MODEL=gpt-4o-mini
```

Option B: Set your API key before starting the server:

```powershell
$env:OPENAI_API_KEY="YOUR_KEY_HERE"
```

Optional model override:

```powershell
$env:OPENAI_MODEL="gpt-4o-mini"
```

### Configuration (optional)

All settings are environment variables:

```text
MAX_FILE_SIZE_MB=10
CORS_ORIGINS=http://localhost:8001
OPENAI_TIMEOUT=30
DEBUG=false
DQA_API_KEY=
CORS_ALLOW_CREDENTIALS=false
```

### Metrics

Get upload metrics at:

```
/api/metrics
```

### 2) Frontend

The frontend is served automatically by the backend at:

```
http://127.0.0.1:8001
```

## Notes
- The `/api/analyze` endpoint accepts file uploads plus optional `analysis_intent` and `target_column` fields.
- The `/api/report/csv` and `/api/report/pdf` endpoints accept a JSON payload with the full analysis output.
- Supported files: `.csv`, `.tsv`, `.xlsx`, `.json`, `.xml`, `.docx`.


## Deploy to Production

### Option A) Render (recommended)

1. Push this project to GitHub.
2. In Render, create a **Web Service** and point to the repo.
3. Use the included `render.yaml` (Blueprint deploy) or set manually:
   - Environment: `Docker`
   - Health check: `/api/metrics`
4. Set required secrets in Render dashboard:
   - `OPENAI_API_KEY`
   - `AUTH_PASSWORD`
   - optionally `DQA_API_KEY`
5. Update `CORS_ORIGINS` to your production domain.

### Option B) Docker (any cloud VM/container service)

```bash
docker build -t cigma-data-profiler .
docker run --name cigma-data-profiler -p 8000:8000 --env-file .env cigma-data-profiler
```

### Recommended Production Env

Use `.env.example` as template. At minimum set:
- `OPENAI_API_KEY`
- `AUTH_REQUIRED=true`
- `AUTH_PASSWORD=<strong-password>`
- `CORS_ORIGINS=https://your-domain`

### Notes
- Do not commit `.env`.
- If any API key was exposed, revoke and rotate it before deploy.
- Backend serves frontend directly, so one service deploy is enough.
