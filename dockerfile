FROM apache/airflow:3.1.0-python3.12

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY .env /opt/airflow/.env
