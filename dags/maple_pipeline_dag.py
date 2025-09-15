# dags/maple_pipeline_dag.py
from datetime import timedelta
import pendulum
from airflow import DAG

from ranking_operator import MapleRankingToS3Operator
from merge_names_operator import MapleMergeDedupNamesOperator
from ocid_operator import MapleOcidOperator
from basic_operator import MapleCharacterBasicOperator

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="maple_pipeline",
    description="메분기 데이터 파이프라인",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, 0, 0, tz=KST),
    schedule_interval="0 7 * * 5",  # 매주 금요일 07:00
    catchup=False,
    tags=["maple", "etl", "nexon"],
    timezone=KST,
) as dag:
    fetch_ranking_world0 = MapleRankingToS3Operator(
        task_id="fetch_ranking_world0",
        target_ymd="{{ ds }}",
        world_type=0,
    )
    fetch_ranking_world1 = MapleRankingToS3Operator(
        task_id="fetch_ranking_world1",
        target_ymd="{{ ds }}",
        world_type=1,
    )
    merge_dedup_names = MapleMergeDedupNamesOperator(
        task_id="merge_dedup_names",
        target_ymd="{{ ds }}",
        input_keys=[
            "staging/ranking_names/{{ ds }}/world0.jsonl",
            "staging/ranking_names/{{ ds }}/world1.jsonl",
        ],
    )
    map_names_to_ocid = MapleOcidOperator(
        task_id="map_names_to_ocid",
        target_ymd="{{ ds }}",
        input_key="staging/ranking_names/{{ ds }}/merged.jsonl",
    )
    fetch_character_basic = MapleCharacterBasicOperator(
        task_id="fetch_character_basic",
        target_ymd="{{ ds }}",
        input_key="staging/ocids/{{ ds }}/ocids.jsonl",
    )

    [fetch_ranking_world0, fetch_ranking_world1] >> merge_dedup_names
    merge_dedup_names >> map_names_to_ocid >> fetch_character_basic
