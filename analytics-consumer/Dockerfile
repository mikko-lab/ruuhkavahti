FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Vendoroidut jaetut moduulit (ks. shared/README.md) — sama periaate kuin
# guardrail/vendor/refuse_dont_guess, ei asenneta pakettina vaan kopioidaan.
COPY vendor ./vendor
COPY analytics_consumer.py .

CMD ["python", "analytics_consumer.py"]
