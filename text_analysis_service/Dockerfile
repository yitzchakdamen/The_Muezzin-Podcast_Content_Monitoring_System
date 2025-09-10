FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY text_analysis_service /app/text_analysis_service

COPY config /app/config
COPY utils /app/utils

CMD ["python", "-m", "text_analysis_service.main"]
