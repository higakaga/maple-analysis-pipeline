import boto3
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils import BAN_NAMES, is_challenger_world, s3_read_jsonl, s3_put_jsonl, S3_BUCKET


class MapleMergeDedupNamesOperator(BaseOperator):
    @apply_defaults
    def __init__(self, target_ymd, world0_key, world1_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_bucket = S3_BUCKET
        self.target_ymd = target_ymd
        self.world0_key = world0_key
        self.world1_key = world1_key

    def execute(self, context):
        s3 = boto3.client("s3")
        rows0 = s3_read_jsonl(s3, self.s3_bucket,
                              self.world0_key) if self.world0_key else []
        rows1 = s3_read_jsonl(s3, self.s3_bucket,
                              self.world1_key) if self.world1_key else []
        rows = rows0 + rows1

        grouped = {}
        for r in rows:
            name = (r.get("character_name") or "").strip()
            if name in BAN_NAMES:
                continue
            grouped.setdefault(name, []).append(r)

        dedup = []
        for name, items in grouped.items():
            if len(items) == 1:
                dedup.append({"character_name": name})
                continue
            non_chall = [it for it in items if not is_challenger_world(
                it.get("world_name") or "")]
            candidates = non_chall if non_chall else items
            chosen = sorted(
                candidates, key=lambda x: x.get("ranking") or 1e9)[0]
            dedup.append({"character_name": chosen["character_name"]})

        out_key = f"staging/ranking_names/{self.target_ymd}/names_dedup.jsonl"
        s3_put_jsonl(s3, self.s3_bucket, out_key, dedup)
        return out_key
