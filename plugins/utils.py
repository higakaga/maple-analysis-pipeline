import os
import time
import json
import boto3
import requests
from botocore.exceptions import ClientError
from airflow.models import Variable
from threading import Lock
from collections import deque

# Airflow Variables

S3_BUCKET = Variable.get("MAPLE_S3_BUCKET")
# 초당 400회 (MAX가 초당 500회이므로 안전하게)
MAX_QPS = int(Variable.get("MAPLE_API_QPS", 400))
NUM_WORKERS = int(Variable.get("MAPLE_API_MAX_WORKERS", 64))  # 병렬 워커 수
RETRIES = 5
TIMEOUT = 10  # 초 단위 요청 타임아웃


# Rate Limiter (Token Bucket)


class RateLimiter:
    """
    Token bucket 방식, 초당 MAX_QPS 제한 보장
    """

    def __init__(self, qps: int):
        self.qps = qps
        self.tokens = deque()
        self.lock = Lock()
        self._last_refill = time.time()

    def acquire(self):
        """
        호출 시 토큰 확보. 없으면 대기.
        """
        while True:
            with self.lock:
                now = time.time()
                # 1초마다 토큰 리필
                if now - self._last_refill >= 1:
                    self.tokens.clear()
                    for _ in range(self.qps):
                        self.tokens.append(1)
                    self._last_refill = now
                if self.tokens:
                    self.tokens.popleft()
                    return
            time.sleep(0.001)  # busy wait 방지


rate_limiter = RateLimiter(MAX_QPS)


# Nexon API


def requests_get_with_retry(url, params, api_key, max_retries=RETRIES):
    """
    GET 요청 + 재시도
    - 2xx: 성공
    - 4xx: 클라이언트 오류 → 즉시 실패
    - 5xx: 서버 오류 → 지수 백오프 재시도
    """
    headers = {"x-nxopen-api-key": api_key}
    backoff = 1.0

    for attempt in range(max_retries):
        rate_limiter.acquire()
        try:
            resp = requests.get(url, params=params,
                                headers=headers, timeout=TIMEOUT)

            # 성공
            if resp.status_code == 200:
                return resp, None

            # 클라이언트 오류는 재시도 의미 없음
            if 400 <= resp.status_code < 500:
                return resp, f"Client error {resp.status_code}: {resp.text}"

            # 서버 오류 → 백오프 후 재시도
            if 500 <= resp.status_code < 600:
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue

            return resp, f"Unexpected status {resp.status_code}: {resp.text}"

        except requests.RequestException as e:
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
            last_err = str(e)

    return None, f"Failed after {max_retries} retries. Last error: {last_err}"


# S3 Helper

def s3_client():
    return boto3.client("s3")


def s3_put_json(s3, bucket, key, data):
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode("utf-8")
        )
    except ClientError as e:
        raise RuntimeError(f"S3 put_json failed: {bucket}/{key}, {e}")


def s3_put_jsonl(s3, bucket, key, rows):
    try:
        body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode("utf-8")
        )
    except ClientError as e:
        raise RuntimeError(f"S3 put_jsonl failed: {bucket}/{key}, {e}")


def s3_read_jsonl(s3, bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
        return [json.loads(line) for line in body.strip().splitlines() if line.strip()]
    except ClientError as e:
        raise RuntimeError(f"S3 read_jsonl failed: {bucket}/{key}, {e}")
