# Build context is the REPO ROOT:
#
#   docker build -f .dagu/Dockerfile -t broksum-runtime:latest 
#
# The environment only — no application code. The workflow checks the repo out
# into /workspace with `runners clone`, so only a dependency change needs this
# rebuilt

FROM python:3.11-slim

WORKDIR /workspace

ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Jakarta

# tzdata: so TZ above means something. git: `runners clone` needs it
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git tzdata \
    && rm -rf /var/lib/apt/lists/*

# Playwright is only for auth_bootstrap.py, the one-time login
# nothing in a container opens a browser, so drop it. segno draws the login QR
# and is not in requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN grep -vi '^playwright' /tmp/requirements.txt > /tmp/requirements.container.txt \
    && pip install --no-cache-dir -r /tmp/requirements.container.txt segno \
    && rm /tmp/requirements*.txt
