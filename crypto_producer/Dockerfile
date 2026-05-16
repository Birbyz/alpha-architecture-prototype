FROM python:3.11-slim

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY producer.py /app/producer.py

# Run
CMD ["python", "/app/producer.py"]
