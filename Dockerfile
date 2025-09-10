FROM apache/airflow:2.9.2-python3.12
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt