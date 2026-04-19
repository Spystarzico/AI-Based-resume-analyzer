FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY resume_analyzer/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY resume_analyzer/backend .

EXPOSE 10000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "10000"]