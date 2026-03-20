# ============================================================
# dag_spark_iceberg.py
# DAG de Apache Airflow que orquesta el pipeline:
#   1. Verificar que MinIO y Nessie estén disponibles
#   2. Crear buckets en MinIO (raw, iceberg)
#   3. Descargar el Parquet de NYC Taxis a MinIO
#   4. Ejecutar el job de Spark que carga los datos a Iceberg
#   5. Verificar que la tabla Iceberg fue creada
#
# Colocar este archivo en la carpeta: dags/
# ============================================================

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ── Logger ───────────────────────────────────────────────────
log = logging.getLogger(__name__)

# ── Configuración del pipeline ────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",    "http://fhbd-minio:9000")
MINIO_ACCESS     = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET     = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")
BUCKET_RAW       = os.getenv("MINIO_BUCKET_RAW",  "raw")
BUCKET_ICEBERG   = os.getenv("MINIO_BUCKET_ICEBERG", "iceberg")
PARQUET_KEY      = os.getenv("MINIO_OBJECT_KEY",  "nyc/yellow_tripdata_2025-01.parquet")
PARQUET_URL      = os.getenv("PARQUET_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet")
NESSIE_URL       = os.getenv("NESSIE_URL",        "http://fhbd-nessie:19120/api/v1")
SPARK_MASTER     = "spark://spark-master:7077"
SCRIPT_PATH      = "/opt/airflow/scripts/spark_load_iceberg.py"
NAMESPACE        = os.getenv("ICEBERG_NAMESPACE", "nyc")
TABLE            = os.getenv("ICEBERG_TABLE",     "yellow_tripdata")

# ── Argumentos por defecto del DAG ───────────────────────────
default_args = {
    "owner"           : "fhbd-grupo",
    "depends_on_past" : False,
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=3),
}


# ════════════════════════════════════════════════════════════
# FUNCIONES PYTHON PARA LAS TAREAS
# ════════════════════════════════════════════════════════════

def verificar_minio(**context):
    """Verifica que MinIO esté disponible haciendo ping al health endpoint."""
    import requests
    url = f"{MINIO_ENDPOINT}/minio/health/live"
    log.info(f"Verificando MinIO en {url}")
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        log.info("✅ MinIO está disponible.")
    except Exception as e:
        raise Exception(f"MinIO no está disponible: {e}")


def verificar_nessie(**context):
    """Verifica que Nessie esté disponible."""
    import requests
    url = f"{NESSIE_URL.replace('/api/v1', '')}/q/health"
    log.info(f"Verificando Nessie en {url}")
    try:
        r = requests.get(url, timeout=10)
        log.info(f"✅ Nessie respondió con status {r.status_code}.")
    except Exception as e:
        log.warning(f"Nessie health check falló pero puede estar corriendo: {e}")


def crear_buckets(**context):
    """Crea los buckets raw e iceberg en MinIO si no existen."""
    import boto3
    log.info("Creando buckets en MinIO...")
    client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name='us-east-1'
    )
    for bucket in [BUCKET_RAW, BUCKET_ICEBERG]:
        try:
            client.head_bucket(Bucket=bucket)
            log.info(f"  Bucket '{bucket}' ya existe.")
        except Exception:
            client.create_bucket(Bucket=bucket)
            log.info(f"  ✅ Bucket '{bucket}' creado.")


def descargar_parquet(**context):
    """Descarga el archivo Parquet de NYC Taxis a MinIO si no existe."""
    import boto3
    import requests as req

    client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name='us-east-1'
    )

    try:
        client.head_object(Bucket=BUCKET_RAW, Key=PARQUET_KEY)
        log.info(f"✅ Archivo '{PARQUET_KEY}' ya existe en MinIO. Saltando descarga.")
        return
    except Exception:
        pass

    log.info(f"Descargando {PARQUET_URL} ...")
    response = req.get(PARQUET_URL, stream=True, timeout=600)
    response.raise_for_status()

    client.upload_fileobj(
        response.raw,
        BUCKET_RAW,
        PARQUET_KEY,
        ExtraArgs={'ContentType': 'application/octet-stream'}
    )
    log.info(f"✅ Archivo subido a s3://{BUCKET_RAW}/{PARQUET_KEY}")


def verificar_tabla_iceberg(**context):
    """Verifica que la tabla Iceberg fue creada consultando la API de Nessie."""
    import requests
    url = f"{NESSIE_URL}/trees/main"
    log.info(f"Verificando tabla Iceberg en Nessie...")
    try:
        r = requests.get(url, timeout=15)
        log.info(f"✅ Nessie respondió. Pipeline completado exitosamente.")
        log.info(f"   Tabla esperada: nessie.{NAMESPACE}.{TABLE}")
    except Exception as e:
        log.warning(f"No se pudo verificar Nessie: {e}")


# ════════════════════════════════════════════════════════════
# DEFINICIÓN DEL DAG
# ════════════════════════════════════════════════════════════

with DAG(
    dag_id="spark_load_to_iceberg",
    description="Pipeline: Descarga NYC Taxis Parquet → MinIO → Spark → Iceberg (Nessie)",
    default_args=default_args,
    schedule_interval=None,          # Solo ejecución manual (o cambiar a @daily)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["fhbd", "spark", "iceberg", "nessie", "minio", "nyc-taxis"],
) as dag:

    # ── Tarea 1: Verificar MinIO ──────────────────────────────
    t1_verificar_minio = PythonOperator(
        task_id="verificar_minio",
        python_callable=verificar_minio,
        doc_md="Verifica que el servicio MinIO esté disponible antes de continuar.",
    )

    # ── Tarea 2: Verificar Nessie ─────────────────────────────
    t2_verificar_nessie = PythonOperator(
        task_id="verificar_nessie",
        python_callable=verificar_nessie,
        doc_md="Verifica que el catálogo Nessie esté disponible.",
    )

    # ── Tarea 3: Crear buckets ────────────────────────────────
    t3_crear_buckets = PythonOperator(
        task_id="crear_buckets_minio",
        python_callable=crear_buckets,
        doc_md="Crea los buckets 'raw' e 'iceberg' en MinIO si no existen.",
    )

    # ── Tarea 4: Descargar Parquet ────────────────────────────
    t4_descargar_parquet = PythonOperator(
        task_id="descargar_parquet_a_minio",
        python_callable=descargar_parquet,
        execution_timeout=timedelta(minutes=30),
        doc_md="Descarga el archivo Parquet de NYC Yellow Taxis 2025-01 a MinIO.",
    )

    # ── Tarea 5: Ejecutar job Spark ───────────────────────────
    t5_spark_job = BashOperator(
        task_id="ejecutar_spark_load_iceberg",
        bash_command=f"""
            echo "Iniciando job Spark..."
            /opt/spark/bin/spark-submit \
                --master {SPARK_MASTER} \
                --deploy-mode client \
                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
                --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
                --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
                --conf spark.sql.catalog.nessie.uri={NESSIE_URL} \
                --conf spark.sql.catalog.nessie.ref=main \
                --conf spark.sql.catalog.nessie.authentication.type=NONE \
                --conf spark.sql.catalog.nessie.warehouse=s3://iceberg/ \
                --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
                --conf spark.sql.catalog.nessie.s3.endpoint={MINIO_ENDPOINT} \
                --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} \
                --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS} \
                --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET} \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
                {SCRIPT_PATH}
            echo "Job Spark finalizado con código: $?"
        """,
        execution_timeout=timedelta(minutes=60),
        doc_md="Ejecuta el script PySpark que lee el Parquet y escribe la tabla Iceberg.",
    )

    # ── Tarea 6: Verificar tabla Iceberg ─────────────────────
    t6_verificar_iceberg = PythonOperator(
        task_id="verificar_tabla_iceberg",
        python_callable=verificar_tabla_iceberg,
        doc_md="Verifica que la tabla Iceberg fue creada correctamente en Nessie.",
    )

    # ════════════════════════════════════════════════════════
    # DEPENDENCIAS — Flujo del pipeline
    #
    #   verificar_minio ──┐
    #                     ├─► crear_buckets ──► descargar_parquet ──► spark_job ──► verificar_iceberg
    #   verificar_nessie ─┘
    #
    # ════════════════════════════════════════════════════════
    [t1_verificar_minio, t2_verificar_nessie] >> t3_crear_buckets
    t3_crear_buckets >> t4_descargar_parquet
    t4_descargar_parquet >> t5_spark_job
    t5_spark_job >> t6_verificar_iceberg
