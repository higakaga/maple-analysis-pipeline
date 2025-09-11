#!/bin/bash
set -e

APP_DIR="/home/ubuntu/airflow"
cd $APP_DIR

# .env 파일 생성
cat > .env <<EOF
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${RDS_USER}:${RDS_PASSWORD}@${RDS_HOST}:5432/${RDS_DB}
EOF

mkdir -p ./dags ./logs ./plugins
sudo chown -R 50000:50000 ./dags ./logs ./plugins

# DB 초기화 (최초 실행 시만 적용)
(docker compose run --rm airflow-init || true) 2>/dev/null || true

docker compose up -d --build

# 헬스체크
sleep 15
if curl -fsS http://localhost:8080/health; then
  echo "Airflow Webserver 배포 완료"
else
  echo "Airflow Webserver Health check 실패"
  docker compose logs --no-color
  exit 1
fi
