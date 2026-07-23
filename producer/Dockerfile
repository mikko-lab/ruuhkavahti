FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY vendor ./vendor
COPY producer.py .

EXPOSE 8001
CMD ["python", "producer.py"]
