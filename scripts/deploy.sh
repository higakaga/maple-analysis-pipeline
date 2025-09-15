#!/bin/bash
set -e

APP_DIR="/home/ubuntu/airflow"
cd $APP_DIR

echo "[INFO] Fetching parameters from AWS SSM..."

# AWS SSM Parameter Store에서 값 가져오기 (SecureString은 --with-decryption 사용)
RDS_HOST=$(aws ssm get-parameter --name "/maple/RDS_HOST" --query "Parameter.Value" --output text)
RDS_USER=$(aws ssm get-parameter --name "/maple/RDS_USER" --query "Parameter.Value" --output text)
RDS_PASSWORD=$(aws ssm get-parameter --with-decryption --name "/maple/RDS_PASSWORD" --query "Parameter.Value" --output text)
RDS_DB=$(aws ssm get-parameter --name "/maple/RDS_DB" --query "Parameter.Value" --output text)
NEXON_API_KEY=$(aws ssm get-parameter --with-decryption --name "/maple/NEXON_API_KEY" --query "Parameter.Value" --output text)

MAPLE_S3_BUCKET="maple-analysis-data-s3"
MAPLE_API_QPS=400
MAPLE_API_MAX_WORKERS=10

echo "[INFO] Creating .env file..."

# .env 파일 생성
cat > .env <<EOF
RDS_HOST=$RDS_HOST
RDS_USER=$RDS_USER
RDS_PASSWORD=$RDS_PASSWORD
RDS_DB=$RDS_DB

MAPLE_S3_BUCKET=$MAPLE_S3_BUCKET
MAPLE_API_QPS=$MAPLE_API_QPS
MAPLE_API_MAX_WORKERS=$MAPLE_API_MAX_WORKERS
NEXON_API_KEY=$NEXON_API_KEY

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://$RDS_USER:$RDS_PASSWORD@$RDS_HOST:5432/$RDS_DB
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
EOF

mkdir -p ./dags ./logs ./plugins
sudo chown -R 50000:50000 ./dags ./logs ./plugins

# DB 초기화 (최초 실행 시만 적용)
(docker compose run --rm airflow-init || true) 2>/dev/null || true

docker compose down -v || true
docker compose up -d --build