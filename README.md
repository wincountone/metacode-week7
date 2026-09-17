# metacode-week7

Q3 ~ Q10 실습을 위한 Airflow Docker Compose 작업 환경입니다.

## 구성

- `docker-compose.yaml`: Airflow 공식 문서에서 내려받은 compose 파일 (CeleryExecutor)
- `.env`: `AIRFLOW_UID` 설정
- `dags/`: 실습 DAG 모음

## 실습 환경

- (feature) Airflow 3.3.2 공식 compose를 Q5에서 커스텀 이미지로 재빌드해 Q3~Q10을 컨테이너 하나로 이어서 진행했다. Spark는 Q4에서 별도 클러스터, Q9에서는 컨테이너 안 pyspark로 처리.
- AWS는 IAM 사용자(edu_001, root 아님)로 접근하고, 액세스 키는 코드/yaml/.env 어디에도 적지 않고 호스트 ~/.aws를 컨테이너에 read-only로 마운트해서 사용. 버킷은 de-3-seungsoohan.

## 회고

1. (feature) 동시 실행으로 Spark 쓰기가 충돌났다 — scheduled 실행과 manual 실행이 같은 로컬 parquet 경로에 동시에 쓰려다 FileFormatWriter 커밋 충돌로 실패. run_id별 경로 분리로 해결.
2. Airflow 3 manual trigger의 logical_date=None 문제 — upload_silver 태스크에서 context["ds"]로 날짜를 꺼내 S3 prefix(silver/{ds}/)를 만들었는데, 수동 트리거는 logical_date가 None이라 ds 키 자체가 context에 없어서 KeyError: 'ds'로 실패했습니다. context.get("ds")가 없으면 현재 시각을 직접 계산하는 방식으로 고쳤습니다.