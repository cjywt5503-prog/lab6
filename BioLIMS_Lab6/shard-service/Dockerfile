FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY order-sample-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY lab6_scripts ./lab6_scripts
COPY shard-service/app.py ./app.py
CMD ["python", "app.py"]
