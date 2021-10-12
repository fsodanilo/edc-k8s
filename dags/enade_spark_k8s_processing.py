from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
glue = boto3.client('glue', region_name='us-east-2',
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)

from airflow.utils.dates import days_ago

def trigger_crawler_enade2019_sup_aluno():
    glue.start_crawler(Name='enade_superior')

def trigger_crawler_enade2019_sup_docente():
    glue.start_crawler(Name='enade_docente')

def trigger_crawler_enade2019_sup_curso():
    glue.start_crawler(Name='enade_curso')

with DAG(
    'enade_batch_spark_k8s',
    default_args={
        'owner': 'Danilo',
        'depends_on_past': False,
        'email': ['fsodanilo@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='Processamento do Censo da Educação Superior 2019 - Inep (Alunos, Docentes e Cursos)',
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch', 'enade-superior-2019'],
) as dag:
    extracao = KubernetesPodOperator(
        namespace='airflow',
        image="284165992806.dkr.ecr.us-east-2.amazonaws.com/extraction-edsup-2019:v2",
        cmds=["python", "/run.py"],
        name="extraction-edsup-2019",
        task_id="extraction-edsup-2019",
        image_pull_policy="Always",
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
    )


    converte_docente_parquet = SparkKubernetesOperator(
        task_id='converte_docente_parquet',
        namespace="airflow",
        application_file="enade_converte_docente.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_docente_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_docente_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_docente_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    converte_aluno_parquet = SparkKubernetesOperator(
        task_id='converte_aluno_parquet',
        namespace="airflow",
        application_file="enade_converte_aluno.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_aluno_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_aluno_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_aluno_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )
    
    
    converte_curso_parquet = SparkKubernetesOperator(
        task_id='converte_curso_parquet',
        namespace="airflow",
        application_file="enade_converte_curso.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_curso_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_curso_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_curso_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    trigger_crawler_enade_sup_aluno = PythonOperator(
        task_id='trigger_crawler_ENADE2019_SUP_ALUNO',
        python_callable=trigger_crawler_enade2019_sup_aluno,
    )
    
    trigger_crawler_enade_sup_docente = PythonOperator(
        task_id='trigger_crawler_ENADE2019_SUP_DOCENTE',
        python_callable=trigger_crawler_enade2019_sup_docente,
    )

    trigger_crawler_enade_sup_curso = PythonOperator(
        task_id='trigger_crawler_ENADE2019_SUP_CURSO',
        python_callable=trigger_crawler_enade2019_sup_curso,
    )


extracao >> [converte_curso_parquet, converte_docente_parquet]
converte_curso_parquet >> converte_curso_parquet_monitor >> trigger_crawler_enade_sup_curso
converte_docente_parquet >> converte_docente_parquet_monitor >> trigger_crawler_enade_sup_docente
converte_docente_parquet_monitor >> converte_aluno_parquet >> converte_aluno_parquet_monitor >> trigger_crawler_enade_sup_aluno