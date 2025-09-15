import os
import time
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from utils import requests_get_with_retry, s3_client, s3_read_jsonl, s3_put_jsonl


class MapleOcidOperator(BaseOperator):
    """
    character_name → ocid 매핑
    실패한 캐릭터는 failed.jsonl 로 저장
    """

    @apply_defaults
    def __init__(self, target_ymd: str, input_key: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_ymd = target_ymd
        self.input_key = input_key
        self.api_key = Variable.get("NEXON_API_KEY")
        self.bucket = Variable.get("MAPLE_S3_BUCKET")
        self.max_workers = int(Variable.get(
            "MAPLE_API_MAX_WORKERS", default_var="32"))
        self.qps_limit = int(Variable.get("MAPLE_API_QPS", default_var="400"))
        self.base_url = "https://open.api.nexon.com/maplestory/v1/id"

        # QPS 제어
        self._last_called = 0
        self._lock = None

    def _rate_limit(self):
        """QPS 제어: 초당 호출 수 제한, MAX 500"""
        now = time.time()
        if self._last_called and (now - self._last_called) < 1.0 / self.qps_limit:
            time.sleep(1.0 / self.qps_limit)
        self._last_called = time.time()

    def _fetch_ocid(self, row: Dict[str, Any]) -> Dict[str, Any]:
        self._rate_limit()
        params = {"character_name": row["character_name"]}
        resp, err = requests_get_with_retry(
            self.base_url, params, self.api_key)
        if resp and resp.status_code == 200:
            return {
                "character_name": row["character_name"],
                "world_name": row.get("world_name"),
                "ocid": resp.json().get("ocid"),
            }
        return None

    def execute(self, context):
        s3 = s3_client()
        rows = s3_read_jsonl(s3, self.bucket, self.input_key)

        ocid_rows, failed_rows = [], []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_row = {executor.submit(
                self._fetch_ocid, r): r for r in rows}
            for future in as_completed(future_to_row):
                row = future_to_row[future]
                try:
                    result = future.result()
                    if result and result.get("ocid"):
                        ocid_rows.append(result)
                    else:
                        failed_rows.append(row)
                except Exception:
                    failed_rows.append(row)

        out_key = f"staging/ocids/{self.target_ymd}/ocids.jsonl"
        s3_put_jsonl(s3, self.bucket, out_key, ocid_rows)

        if failed_rows:
            fail_key = f"staging/ocids/{self.target_ymd}/failed.jsonl"
            s3_put_jsonl(s3, self.bucket, fail_key, failed_rows)
            self.log.warning(
                f"ocid 변환 실패 → s3://{self.bucket}/{fail_key}")

        self.log.info(
            f"ocid 변환 작업 결과 {len(ocid_rows)}명 성공, {len(failed_rows)}명 실패")
        return out_key
