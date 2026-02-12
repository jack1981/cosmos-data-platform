FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY apps/management_api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY apps/management_api /app
COPY cosmos_xenna /workspace/cosmos_xenna

ENV PYTHONPATH=/workspace

EXPOSE 8000

CMD ["/app/entrypoint.sh"]
