FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN pip install --no-cache-dir fastapi==0.110.0 uvicorn==0.29.0
COPY incident-service/app.py ./app.py
CMD ["python", "app.py"]
