# Build context is the REPO ROOT, not this directory, because the image needs
# both ./services/auth and the shared ./common package:
#   docker build -f services/auth/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install deps before copying source so code edits do not invalidate the layer.
COPY services/auth/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Shared library, installed editable so it is importable as `common`.
COPY common/ /libs/common/
RUN pip install --no-cache-dir -e /libs/common

COPY services/auth/ /app/

RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["api"]
