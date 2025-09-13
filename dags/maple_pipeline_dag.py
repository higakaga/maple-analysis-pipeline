from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.utils import timezone

from ranking_operator import MapleRankingToS3Operator
from merge_operator import MapleMergeDedupNamesOperator
from ocid_operator import MapleOcidOperator
from character_operator import MapleCharacterBasicOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="maple_pipeline",
    description="MapleStory pipeline: ranking -> id -> character/basic",
    default_args=default_args,
    schedule_interval="0 22 * * FRI",  # 금 오전 7시마다 실행
    start_date=days_ago(1),
    catchup=False,
    tags=["maple", "nexon", "s3", "rds"],
) as dag:

    # 실행일 기준 하루 전 날짜로 호출
    @task
    def compute_target_ymd(execution_date=None):
        kst = timezone.convert_to_timezone(
            execution_date or timezone.utcnow(), "Asia/Seoul")
        target = (kst - timedelta(days=1)).date()
        return target.strftime("%Y-%m-%d")

    target_ymd = compute_target_ymd()

    # 1. 일반 월드(월드타입 0) 랭킹 데이터 수집
    ranking0 = MapleRankingToS3Operator(
        task_id="fetch_ranking_world0",
        target_ymd=target_ymd,
        world_type=0,
    )

    # 2. 에오스/헬리오스(월드타입 1) 랭킹 데이터 수집
    ranking1 = MapleRankingToS3Operator(
        task_id="fetch_ranking_world1",
        target_ymd=target_ymd,
        world_type=1,
    )

    # 3. 랭킹 데이터 병합 + 중복 제거 (챌린저스 리프)
    merge = MapleMergeDedupNamesOperator(
        task_id="merge_dedup_names",
        target_ymd=target_ymd,
        world0_key=ranking0.output,
        world1_key=ranking1.output,
    )

    # 4. 캐릭터명 → ocid 매핑
    ocid = MapleOcidOperator(
        task_id="map_names_to_ocid",
        target_ymd=target_ymd,
        names_key=merge.output,
    )

    # 5. ocid → 캐릭터 기본 정보 조회 + RDS 적재
    char_basic = MapleCharacterBasicOperator(
        task_id="fetch_character_basic",
        target_ymd=target_ymd,
        id_map_key=ocid.output,
    )

    [ranking0, ranking1] >> merge >> ocid >> char_basic
