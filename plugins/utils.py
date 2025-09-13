import os
import json
import time
import requests
import boto3
from airflow.models import Variable

# 빈 문자열만 제거
BAN_NAMES = {""}
CHALLENGER_WORDS = ("챌린저스", "챌린저스2", "챌린저스3", "챌린저스4")

# Airflow Variables (EC2에서 조정 가능)
S3_BUCKET = Variable.get("MAPLE_S3_BUCKET", default_var="maple-genesis-data")
POSTGRES_CONN_ID = Variable.get(
    "MAPLE_RDS_CONN_ID", default_var="maple_postgres")

MAX_QPS = int(Variable.get("MAPLE_MAX_QPS", default_var=400))
NUM_WORKERS = int(Variable.get("MAPLE_NUM_WORKERS", default_var=10))
SLEEP_INTERVAL = 1.0 / (MAX_QPS / NUM_WORKERS)


def is_challenger_world(world_name: str) -> bool:
    return any(word in (world_name or "") for word in CHALLENGER_WORDS)


def requests_get_with_retry(url, params, api_key, max_retries=5, base_sleep=1.0):
    headers = {"x-nxopen-api-key": api_key}
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params,
                                headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp, None
            elif resp.status_code in (400, 403):
                try:
                    err = resp.json().get("error", {})
                    return None, f"{resp.status_code}: {err.get('name')} - {err.get('message')}"
                except Exception:
                    return None, f"{resp.status_code}: {resp.text[:200]}"
            elif resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(
                    retry_after) if retry_after else base_sleep * attempt
                time.sleep(sleep_s)
            elif 500 <= resp.status_code < 600:
                time.sleep(base_sleep * attempt)
            else:
                return None, f"{resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            last_err = str(e)
            time.sleep(base_sleep * attempt)
    return None, last_err or "unknown_error"


def s3_put_json(client, bucket, key, obj):
    body = json.dumps(obj, ensure_ascii=False)
    client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def s3_put_jsonl(client, bucket, key, rows):
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def s3_read_jsonl(client, bucket, key):
    obj = client.get_object(Bucket=bucket, Key=key)
    return [json.loads(line) for line in obj["Body"].read().decode("utf-8").splitlines() if line.strip()]
