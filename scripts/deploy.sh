#!/bin/bash
set -e

APP_DIR="/home/ubuntu/airflow"
cd $APP_DIR

# 폴더 권한 airflow로
mkdir -p ./dags ./logs ./plugins
sudo chown -R 50000:50000 ./dags ./logs ./plugins

# DB 초기화 (최초 실행 시만 적용)
(docker compose run --rm airflow-init || true) 2>/dev/null || true

# 컨테이너 재시작
docker compose down -v || true
docker compose up -d --build