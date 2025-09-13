import os
import time
import boto3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.utils import (
    requests_get_with_retry, s3_put_json, s3_put_jsonl, s3_read_jsonl,
    BAN_NAMES, S3_BUCKET, POSTGRES_CONN_ID, MAX_QPS, NUM_WORKERS, SLEEP_INTERVAL
)


class MapleCharacterBasicOperator(BaseOperator):
    ui_color = "#ffab91"

    @apply_defaults
    def __init__(self, target_ymd, id_map_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_bucket = S3_BUCKET
        self.target_ymd = target_ymd
        self.id_map_key = id_map_key
        self.postgres_conn_id = POSTGRES_CONN_ID
        self.api_key = os.getenv("NEXON_API_KEY")
        self.base_url = "https://open.api.nexon.com/maplestory/v1/character/basic"

        self.ok_count = 0
        self.fail_rows = []
        self.lock = threading.Lock()

    def _fetch_character(self, item, s3, hook, upsert_sql):
        ocid = item.get("ocid")
        if not ocid:
            return
        resp, err = requests_get_with_retry(
            self.base_url, {"ocid": ocid, "date": self.target_ymd}, self.api_key)
        if err or resp is None:
            with self.lock:
                self.fail_rows.append(
                    {"snapshot_date": self.target_ymd, "source_api": "character_basic", "fail_key": ocid, "error_message": err})
            return

        data = resp.json()
        if not data or "character_name" not in data:
            with self.lock:
                self.fail_rows.append({"snapshot_date": self.target_ymd, "source_api": "character_basic",
                                      "fail_key": ocid, "error_message": "invalid_response"})
            return

        s3_put_json(s3, self.s3_bucket,
                    f"raw/character_basic/{self.target_ymd}/ocid={ocid}.json", data)

        guild_raw = data.get("character_guild_name")
        guild_clean = None if (guild_raw is None or str(
            guild_raw) in BAN_NAMES) else str(guild_raw)

        row = {
            "snapshot_date": self.target_ymd,
            "character_name": data.get("character_name"),
            "world_name": data.get("world_name"),
            "character_gender": data.get("character_gender"),
            "character_class": data.get("character_class"),
            "character_level": data.get("character_level"),
            "character_guild_name": guild_clean,
            "character_date_create": data.get("character_date_create"),
            "access_flag": data.get("access_flag"),
            "liberation_quest_clear": data.get("liberation_quest_clear"),
        }

        with self.lock:
            hook.run(upsert_sql, parameters=row)
            self.ok_count += 1

        time.sleep(SLEEP_INTERVAL)

    def execute(self, context):
        if not self.api_key:
            raise ValueError("NEXON_API_KEY env not set")

        s3 = boto3.client("s3")
        id_rows = s3_read_jsonl(s3, self.s3_bucket, self.id_map_key)

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        upsert_sql = """
        INSERT INTO character_basic (
            snapshot_date, character_name, world_name, character_gender,
            character_class, character_level, character_guild_name,
            character_date_create, access_flag, liberation_quest_clear, created_at
        ) VALUES (
            %(snapshot_date)s, %(character_name)s, %(world_name)s, %(character_gender)s,
            %(character_class)s, %(character_level)s, %(character_guild_name)s,
            %(character_date_create)s, %(access_flag)s, %(liberation_quest_clear)s, now()
        )
        ON CONFLICT (snapshot_date, character_name) DO UPDATE SET
            world_name=EXCLUDED.world_name,
            character_gender=EXCLUDED.character_gender,
            character_class=EXCLUDED.character_class,
            character_level=EXCLUDED.character_level,
            character_guild_name=EXCLUDED.character_guild_name,
            character_date_create=EXCLUDED.character_date_create,
            access_flag=EXCLUDED.access_flag,
            liberation_quest_clear=EXCLUDED.liberation_quest_clear;
        """

        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = [executor.submit(
                self._fetch_character, item, s3, hook, upsert_sql) for item in id_rows]
            for f in as_completed(futures):
                pass

        if self.fail_rows:
            fail_key = f"errors/character_basic/{self.target_ymd}/failed.jsonl"
            s3_put_jsonl(s3, self.s3_bucket, fail_key, self.fail_rows)
            hook.run(
                "INSERT INTO api_failure_log (snapshot_date, source_api, fail_key, error_message, created_at) VALUES (%s,%s,%s,%s,now())",
                parameters=[(fr["snapshot_date"], fr["source_api"], fr["fail_key"],
                             fr["error_message"]) for fr in self.fail_rows],
            )

        self.log.info(
            f"Character basic complete. ok={self.ok_count}, fail={len(self.fail_rows)}")
        return {"ok": self.ok_count, "fail": len(self.fail_rows)}
