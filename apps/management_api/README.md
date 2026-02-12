# Management API (FastAPI)

## Run locally

```bash
cd apps/management_api
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
alembic upgrade head
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Tests

```bash
cd apps/management_api
pytest -q
```
