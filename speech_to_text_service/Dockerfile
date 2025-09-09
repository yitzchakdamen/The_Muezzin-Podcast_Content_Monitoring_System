FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY speech_to_text_service /app/speech_to_text_service

COPY config /app/config
COPY utils /app/utils

CMD ["python", "-m", "speech_to_text_service.main"]
