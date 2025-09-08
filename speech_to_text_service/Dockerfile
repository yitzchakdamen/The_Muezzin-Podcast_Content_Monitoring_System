FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY processing_metadata_service /app/processing_metadata_service

COPY config /app/config
COPY utils /app/utils
COPY config /app/config
COPY data /app/data

CMD ["python", "-m", "processing_metadata_service.main"]
