import os
import time
import boto3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils import (
    requests_get_with_retry, s3_put_jsonl, s3_read_jsonl,
    BAN_NAMES, S3_BUCKET, POSTGRES_CONN_ID, MAX_QPS, NUM_WORKERS, SLEEP_INTERVAL
)


class MapleOcidOperator(BaseOperator):
    ui_color = "#ffd54f"

    @apply_defaults
    def __init__(self, target_ymd, names_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_bucket = S3_BUCKET
        self.target_ymd = target_ymd
        self.names_key = names_key
        self.postgres_conn_id = POSTGRES_CONN_ID
        self.api_key = os.getenv("NEXON_API_KEY")
        self.base_url = "https://open.api.nexon.com/maplestory/v1/id"

        self.ok_rows = []
        self.fail_rows = []
        self.lock = threading.Lock()

    def _fetch_ocid(self, name):
        if not name or name in BAN_NAMES:
            return
        resp, err = requests_get_with_retry(
            self.base_url, {"character_name": name}, self.api_key)
        if err or resp is None:
            with self.lock:
                self.fail_rows.append({
                    "snapshot_date": self.target_ymd,
                    "source_api": "id",
                    "fail_key": name,
                    "error_message": err or "unknown_error",
                })
        else:
            ocid = (resp.json() or {}).get("ocid")
            if not ocid:
                with self.lock:
                    self.fail_rows.append({
                        "snapshot_date": self.target_ymd,
                        "source_api": "id",
                        "fail_key": name,
                        "error_message": "no_ocid_in_response",
                    })
            else:
                with self.lock:
                    self.ok_rows.append({"character_name": name, "ocid": ocid})
        time.sleep(SLEEP_INTERVAL)

    def execute(self, context):
        if not self.api_key:
            raise ValueError("NEXON_API_KEY env not set")

        s3 = boto3.client("s3")
        names = s3_read_jsonl(s3, self.s3_bucket, self.names_key)
        name_list = [(n.get("character_name") or "").strip() for n in names if (
            n.get("character_name") or "").strip() not in BAN_NAMES]

        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = [executor.submit(self._fetch_ocid, name)
                       for name in name_list]
            for f in as_completed(futures):
                pass

        ok_key = f"raw/id/{self.target_ymd}/id_map.jsonl"
        s3_put_jsonl(s3, self.s3_bucket, ok_key, self.ok_rows)

        if self.fail_rows:
            fail_key = f"errors/id/{self.target_ymd}/failed.jsonl"
            s3_put_jsonl(s3, self.s3_bucket, fail_key, self.fail_rows)

            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            insert_sql = """
            INSERT INTO api_failure_log (snapshot_date, source_api, fail_key, error_message, created_at)
            VALUES (%s, %s, %s, %s, now())
            """
            rows = [(fr["snapshot_date"], fr["source_api"], fr["fail_key"],
                     fr["error_message"]) for fr in self.fail_rows]
            hook.run(insert_sql, parameters=rows)

        self.log.info(
            f"ID mapping complete. ok={len(self.ok_rows)}, fail={len(self.fail_rows)}")
        return ok_key
