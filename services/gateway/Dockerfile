# Build context is the REPO ROOT (needs ./common alongside ./services/gateway):
#   docker build -f services/gateway/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY services/gateway/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /libs/common/
RUN pip install --no-cache-dir -e /libs/common

COPY services/gateway/ /app/

EXPOSE 8000

# Stateless proxy with no migrations to run, so no entrypoint script is needed.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
