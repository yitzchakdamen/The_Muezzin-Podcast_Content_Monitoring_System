FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY api_endpoint_service /app/api_endpoint_service

COPY config /app/config
COPY utils /app/utils

EXPOSE 8000

CMD ["python", "-m", "api_endpoint_service.main"]
