import os
from typing import List, Dict, Any

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

from utils import requests_get_with_retry, s3_client, s3_put_json, s3_put_jsonl


class MapleRankingToS3Operator(BaseOperator):
    """
    /maplestory/v1/ranking/overall 수집
    - date: target_ymd (KST, 'YYYY-MM-DD')
    - world_type: 0 먼저, 그 다음 1
    - page=1부터 증가하다가 ranking 응답 내 character_level < 255 보이면 즉시 중단
    - 원본 페이지 JSON은 raw/ 아래 저장
    - character_name, world_name 등 추출한 이름 리스트를 staging/ 에 JSONL로 저장
    """
    @apply_defaults
    def __init__(
        self,
        target_ymd: str,
        world_type: int,
        max_pages: int = 2000,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.target_ymd = target_ymd
        self.world_type = int(world_type)
        self.max_pages = int(max_pages)

    def execute(self, context):
        api_key = os.getenv("NEXON_API_KEY")
        if not api_key:
            raise ValueError("NEXON_API_KEY 없음")

        bucket = Variable.get("MAPLE_S3_BUCKET")
        s3 = s3_client()

        base_url = "https://open.api.nexon.com/maplestory/v1/ranking/overall"

        names: List[Dict[str, Any]] = []
        stop = False

        for page in range(1, self.max_pages + 1):
            if stop:
                break

            params = {
                "date": self.target_ymd,
                "world_type": str(self.world_type),
                "page": str(page),
            }
            resp, err = requests_get_with_retry(base_url, params, api_key)
            if err or not resp:
                # 원본 실패 건은 로그만 남기고 스킵
                self.log.warning(
                    f"랭킹 정보: world{self.world_type} 실패, 페이지: {page}, 원인: {err}")
                continue

            data = resp.json()

            # raw 저장
            raw_key = f"raw/ranking/{self.target_ymd}/world{self.world_type}/page={page}.json"
            s3_put_json(s3, bucket, raw_key, data)

            # 필요한 필드 추출
            for row in data.get("ranking", []):
                level = row.get("character_level", 0)
                if level < 255:
                    stop = True
                    break

                names.append({
                    "character_name": row.get("character_name"),
                    "world_name": row.get("world_name"),
                    "ranking": row.get("ranking"),
                    "character_level": level,
                })

        stage_key = f"staging/ranking_names/{self.target_ymd}/world{self.world_type}.jsonl"
        s3_put_jsonl(s3, bucket, stage_key, names)
        self.log.info(
            f"랭킹 정보 수집 완료: world{self.world_type}, {len(names)}명 → s3://{bucket}/{stage_key}")
        return stage_key
