import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Заполняем данные
YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCdvRHFq/x/YD8A/3FY8SQiFYhGpFFibvVRpkGxG/I5YlSRkLn0eq8kJzfCRIW2OH+gYXXJ1kmNyvKJTFkYuwZJ/SrrbtmcvrFklEGaIuxng5hcsMXRmxIbsAXP3/onPd5p7dXfb39/Lp15143Hz3MvVsIkvx7cj2bJ8OizHbgHpbCAreyEG0S2PT1nAab3mZqQo7Bkdo3vdf8Cm6OxGL5IUC1f9n9ZzwWlO2sAzE5iHAiaqRQrjuJOsMg9dEwb+ul/DZpbXHGTM3pjTkff+AMI5pAQL6qw1hODAKReeqnHIC/hz1uyg7e2zuAvewhvIV6hwUMTg6cyqT/yFcuJ8Rop admin@DESKTOP-C3MHGAP'  # ВСТАВЬТЕ СВОЙ КЛЮЧ
YC_DP_SUBNET_ID = 'e9bfek4p55d8idarobev'        # ID подсети
YC_DP_SA_ID = 'ajeaa8n8gog1klv6jlva'            # ID сервисного аккаунта
YC_DP_METASTORE_URI = '10.128.0.18'  # IP кластера Hive Metastore
YC_BUCKET = 'etlhw2'                            # Имя бакета

with DAG(
        'DATA_INGEST',
        schedule='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    # 1. Создание временного кластера Data Proc
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для PySpark-задания',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-ssd',
        masternode_disk_size=20,
        computenode_resource_preset='m2.large',
        computenode_disk_type='network-ssd',
        computenode_disk_size=20,
        computenode_count=1,
        computenode_max_hosts_count=2,
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    # 2. Запуск PySpark-задания
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/process_transactions.ipynb',
    )

    # 3. Удаление кластера
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
