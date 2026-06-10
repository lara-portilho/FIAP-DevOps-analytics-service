FROM python:3.11-alpine AS builder

WORKDIR /app

RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.11-alpine AS runner

RUN addgroup -S appgroup
RUN adduser -S -G appgroup -H -s /sbin/nologin appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appgroup /venv /venv
COPY --chown=appuser:appgroup app.py .

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/venv/bin:$PATH"

USER appuser

EXPOSE 8005

CMD ["gunicorn", "--bind", "0.0.0.0:8005", "app:app"]
