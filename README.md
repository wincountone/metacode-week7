# metacode-week7

Q3 ~ Q10 실습을 위한 Airflow Docker Compose 작업 환경입니다.

## 구성

- `docker-compose.yaml`: Airflow 공식 문서에서 내려받은 compose 파일 (CeleryExecutor)
- `.env`: `AIRFLOW_UID` 설정
- `dags/`: 실습 DAG 모음

## 실습 환경

- (main) Airflow 3.3.2 위에 Spark, S3 연동까지 붙여서 Q3~Q10을 하나의 프로젝트 환경으로 계속 이어서 진행했다. 중간에 이미지를 커스텀으로 다시 빌드하고, Spark는 상황에 따라 별도 클러스터와 컨테이너 내부 실행을 섞어 썼다.
- AWS는 IAM 사용자(edu_001, root 아님)로 접근하고, 액세스 키는 코드/yaml/.env 어디에도 적지 않고 호스트 ~/.aws를 컨테이너에 read-only로 마운트해서 사용. 버킷은 de-3-seungsoohan.

## 회고

1. (main) unpause하면서 scheduled 실행과 manual 실행이 동시에 돌아버렸고, 둘 다 같은 로컬 경로에 parquet을 쓰려다 Spark 쪽에서 커밋 충돌이 나서 죽었다. run_id마다 경로를 다르게 줘서 해결.
2. Airflow 3 manual trigger의 logical_date=None 문제 — upload_silver 태스크에서 context["ds"]로 날짜를 꺼내 S3 prefix(silver/{ds}/)를 만들었는데, 수동 트리거는 logical_date가 None이라 ds 키 자체가 context에 없어서 KeyError: 'ds'로 실패했습니다. context.get("ds")가 없으면 현재 시각을 직접 계산하는 방식으로 고쳤습니다.