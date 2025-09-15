import os
import time
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils import requests_get_with_retry, s3_client, s3_read_jsonl


class MapleCharacterBasicOperator(BaseOperator):

    @apply_defaults
    def __init__(self, target_ymd: str, input_key: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_ymd = target_ymd
        self.input_key = input_key
        self.api_key = Variable.get("NEXON_API_KEY")
        self.max_workers = int(Variable.get(
            "MAPLE_API_MAX_WORKERS", default_var="32"))
        self.qps_limit = int(Variable.get("MAPLE_API_QPS", default_var="400"))
        self.base_url = "https://open.api.nexon.com/maplestory/v1/character/basic"
        self.pg_conn_id = Variable.get(
            "MAPLE_RDS_CONN_ID", default_var="maple_postgres")

        # QPS 제어
        self._last_called = 0

    def _rate_limit(self):
        now = time.time()
        if self._last_called and (now - self._last_called) < 1.0 / self.qps_limit:
            time.sleep(1.0 / self.qps_limit)
        self._last_called = time.time()

    def _fetch_basic(self, row: Dict[str, Any]) -> Dict[str, Any]:
        self._rate_limit()
        params = {"ocid": row["ocid"], "date": self.target_ymd}
        resp, err = requests_get_with_retry(
            self.base_url, params, self.api_key)
        if resp and resp.status_code == 200:
            data = resp.json()
            return {
                "ocid": row["ocid"],
                "character_name": data.get("character_name"),
                "world_name": data.get("world_name"),
                "character_gender": data.get("character_gender"),
                "character_class": data.get("character_class"),
                "character_class_level": data.get("character_class_level"),
                "character_level": data.get("character_level"),
                "character_guild_name": data.get("character_guild_name"),
                "character_date_create": data.get("character_date_create"),
                "liberation_quest_clear": data.get("liberation_quest_clear"),
                "snapshot_date": self.target_ymd,
            }
        return None

    def execute(self, context):
        s3 = s3_client()
        rows = s3_read_jsonl(s3, Variable.get(
            "MAPLE_S3_BUCKET"), self.input_key)

        results: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_row = {executor.submit(
                self._fetch_basic, r): r for r in rows}
            for future in as_completed(future_to_row):
                try:
                    res = future.result()
                    if res:
                        results.append(res)
                except Exception:
                    continue

        self.log.info(f"캐릭터 상세정보 조회 {len(results)}명 성공")

        if not results:
            self.log.warning("조회된 캐릭터 정보 없으므로 DB 저장 스킵")
            return None

        pg_hook = PostgresHook(postgres_conn_id=self.pg_conn_id)

        insert_data = [
            (
                r["ocid"],
                r["character_name"],
                r["world_name"],
                r["character_gender"],
                r["character_class"],
                r["character_class_level"],
                r["character_level"],
                r["character_guild_name"],
                r["character_date_create"],
                r["liberation_quest_clear"],
                r["snapshot_date"],
            )
            for r in results
        ]

        pg_hook.insert_rows(
            table="character_basic_from_ranking_for_ocid",
            rows=insert_data,
            target_fields=[
                "ocid", "character_name", "world_name", "character_gender",
                "character_class", "character_class_level", "character_level",
                "character_guild_name", "character_date_create",
                "liberation_quest_clear", "snapshot_date"
            ],
            commit_every=500,
        )

        self.log.info(f"DB에 {len(insert_data)}명 저장 완료")
        return len(insert_data)
