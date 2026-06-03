FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# HuggingFace Spaces expects port 7860
EXPOSE 7860

CMD ["python", "main.py"]
