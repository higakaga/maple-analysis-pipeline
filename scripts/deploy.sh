#!/bin/bash
set -e

APP_DIR="/home/ubuntu/airflow"
cd $APP_DIR

# .env 파일 생성
cat > .env <<EOF
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${RDS_USER}:${RDS_PASSWORD}@${RDS_HOST}:5432/${RDS_META}
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
EOF

mkdir -p ./dags ./logs ./plugins
sudo chown -R 50000:50000 ./dags ./logs ./plugins

# DB 초기화 (최초 실행 시만 적용)
(docker compose run --rm airflow-init || true) 2>/dev/null || true

docker compose down -v || true
docker compose up -d --build