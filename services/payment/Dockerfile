# Build context is the REPO ROOT (needs ./common alongside ./services/payment):
#   docker build -f services/payment/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1     PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY services/payment/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /libs/common/
RUN pip install --no-cache-dir -e /libs/common

COPY services/payment/ /app/

RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["api"]
