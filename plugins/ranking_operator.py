import os
import boto3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.utils import (
    requests_get_with_retry, s3_put_json, s3_put_jsonl,
    S3_BUCKET, MAX_QPS, NUM_WORKERS, SLEEP_INTERVAL
)


class MapleRankingToS3Operator(BaseOperator):
    @apply_defaults
    def __init__(self, target_ymd, world_type, max_pages=2000, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_bucket = S3_BUCKET
        self.target_ymd = target_ymd
        self.world_type = world_type
        self.max_pages = max_pages
        self.api_key = os.getenv("NEXON_API_KEY")
        self.base_url = "https://open.api.nexon.com/maplestory/v1/ranking/overall"

        self.names_rows = []
        self.stop = False
        self.lock = threading.Lock()

    def _fetch_page(self, page, s3):
        if self.stop:
            return

        params = {"date": self.target_ymd, "world_type": str(
            self.world_type), "page": str(page)}
        resp, err = requests_get_with_retry(
            self.base_url, params, self.api_key)
        if err or resp is None:
            return

        data = resp.json()
        s3_put_json(s3, self.s3_bucket,
                    f"raw/ranking/{self.target_ymd}/world{self.world_type}/page={page}.json", data)

        for row in data.get("ranking", []):
            if row.get("character_level", 0) < 255:
                self.stop = True
                break
            with self.lock:
                self.names_rows.append({
                    "character_name": row.get("character_name"),
                    "world_name": row.get("world_name"),
                    "ranking": row.get("ranking"),
                    "character_level": row.get("character_level")
                })

        import time
        time.sleep(SLEEP_INTERVAL)

    def execute(self, context):
        if not self.api_key:
            raise ValueError("NEXON_API_KEY env not set")

        s3 = boto3.client("s3")
        pages = list(range(1, self.max_pages + 1))

        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = [executor.submit(self._fetch_page, p, s3) for p in pages]
            for f in as_completed(futures):
                pass

        stage_key = f"staging/ranking_names/{self.target_ymd}/world{self.world_type}.jsonl"
        s3_put_jsonl(s3, self.s3_bucket, stage_key, self.names_rows)
        return stage_key
