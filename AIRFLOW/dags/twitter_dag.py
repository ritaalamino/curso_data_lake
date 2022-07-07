from datetime import datetime
from os.path import join
from airflow.models import DAG
from airflow.operators.nlp import TwitterOperator

with DAG(dag_id="twitter_dag", start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_nlp_task",
        query="Natural Language Processing",
        file_path=join(
            "/home/ritaalamino/workspace/observatorio/curso_data_lake/AIRFLOW/datalake",
            "twitter_nlp",
            "extract_date={{ ds }}",
            "NLPTask_{{ ds_nodash }}.json"
        ),
    )