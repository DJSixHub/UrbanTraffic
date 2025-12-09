FROM python:3.12-slim

WORKDIR /opt/service

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV PROFILES_PATH=/opt/ml/generated/runtime_distributions.json \
    FALLBACK_PROFILES_PATH=/opt/ml/generated/distributions.json \
    STATIC_PROFILES_PATH=/opt/ml/generated/fallback_distributions.json \
    CLEAN_DATA_PATH=/opt/ml/raw/clean_data.csv \
    MODEL_NAME=baseline-hourly-profile

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
