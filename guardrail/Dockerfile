FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY vendor ./vendor
COPY chat_rule.py pipeline.py dedup.py guardrail_consumer.py ./

CMD ["python", "guardrail_consumer.py"]
