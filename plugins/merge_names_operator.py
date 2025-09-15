from typing import List, Dict, Any
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

from utils import s3_client, s3_read_jsonl, s3_put_jsonl


class MapleMergeDedupNamesOperator(BaseOperator):
    """
    world0 + world1 결과를 병합하며, 동일 character_name이 다수 월드에 있을 때
    챌린저스 계열(챌린저스/챌린저스2/3/4)을 우선 제거.
    """
    @apply_defaults
    def __init__(
        self,
        target_ymd: str,
        input_keys: List[str],
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.target_ymd = target_ymd
        self.input_keys = input_keys

    @staticmethod
    def _is_challengers(world_name: str) -> bool:
        return "챌린저스" in (world_name or "")

    def execute(self, context) -> str:
        bucket = Variable.get("MAPLE_S3_BUCKET")
        s3 = s3_client()

        rows: List[Dict[str, Any]] = []
        for key in self.input_keys:
            rows.extend(s3_read_jsonl(s3, bucket, key))

        # name -> 후보 리스트
        by_name: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            name = r.get("character_name")
            if not name:
                continue
            by_name.setdefault(name, []).append(r)

        merged: List[Dict[str, Any]] = []
        for name, candidates in by_name.items():
            if len(candidates) == 1:
                merged.append(candidates[0])
                continue

            # 챌린저스 아닌 월드 우선
            non_ch = [c for c in candidates if not self._is_challengers(
                c.get("world_name", ""))]
            if non_ch:
                # 랭킹 낮은 값을 우선 선택
                merged.append(
                    sorted(non_ch, key=lambda x: x.get("ranking", 999999))[0])
            else:
                merged.append(candidates[0])

        out_key = f"staging/ranking_names/{self.target_ymd}/merged.jsonl"
        s3_put_jsonl(s3, bucket, out_key, merged)

        # 로그
        dup_count = sum(1 for n, c in by_name.items() if len(c) > 1)
        self.log.info(
            f"캐릭터 중복 제거 결과 원본 {len(rows)}명 → 최종 {len(merged)}명, 중복 {dup_count}명 제거")
        self.log.info(f"병합된 최종 목록 S3 저장 완료 → s3://{bucket}/{out_key}")
        return out_key
