# Troubleshooting

## API cannot connect to Postgres
- Confirm `postgres` is healthy:
  - `docker compose ps`
- Verify env values in `deploy/compose/.env`:
  - `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

## Migration issues
- Run inside API container:
  - `docker compose exec management_api alembic upgrade head`
- If startup failed mid-migration and Postgres now has partial objects, recreate local volumes:
  - `docker compose down -v`
  - `docker compose up --build`

## UI cannot reach API
- Ensure `NEXT_PUBLIC_API_BASE_URL` points to host API URL (`http://localhost:8000/api/v1`).
- Check CORS origin (`FRONTEND_ORIGIN`) includes `http://localhost:3000`.

## Permission denied actions
- Verify user role via `/api/v1/auth/me`.
- Check pipeline ownership/team-sharing and role constraints.
