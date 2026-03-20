# ============================================================
# spark_load_iceberg.py
# Script basado en el notebook Spark-load-to-Iceberg del profe
# Lee Parquet de taxis NYC desde MinIO y lo escribe en Iceberg
# via catálogo Nessie
#
# Uso:
#   spark-submit scripts/spark_load_iceberg.py
# ============================================================

import os
import sys
import boto3
import requests
from pyspark.sql import SparkSession
import pyspark

# ── Configuración desde variables de entorno (o valores por defecto) ──
CATALOG_URI   = os.getenv("NESSIE_URL",        "http://fhbd-nessie:19120/api/v1")
WAREHOUSE     = os.getenv("WAREHOUSE",         "s3://iceberg/")
STORAGE_URI   = os.getenv("MINIO_ENDPOINT",    "http://fhbd-minio:9000")
AWS_ACCESS    = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
AWS_SECRET    = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")
BUCKET_RAW    = os.getenv("MINIO_BUCKET_RAW",  "raw")
BUCKET_ICE    = os.getenv("MINIO_BUCKET_ICEBERG", "iceberg")
PARQUET_KEY   = os.getenv("MINIO_OBJECT_KEY",  "nyc/yellow_tripdata_2025-01.parquet")
PARQUET_URL   = os.getenv("PARQUET_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet")
NAMESPACE     = os.getenv("ICEBERG_NAMESPACE", "nyc")
TABLE         = os.getenv("ICEBERG_TABLE",     "yellow_tripdata")

print("=" * 60)
print("SPARK LOAD TO ICEBERG")
print(f"  Nessie  : {CATALOG_URI}")
print(f"  MinIO   : {STORAGE_URI}")
print(f"  Bucket  : {BUCKET_RAW} -> iceberg/{NAMESPACE}.{TABLE}")
print("=" * 60)


# ── PASO 1: Verificar y crear buckets en MinIO ────────────────
print("\n[1/5] Verificando buckets en MinIO...")

minio_client = boto3.client(
    's3',
    endpoint_url=STORAGE_URI,
    aws_access_key_id=AWS_ACCESS,
    aws_secret_access_key=AWS_SECRET,
    region_name='us-east-1'
)

for bucket in [BUCKET_RAW, BUCKET_ICE]:
    try:
        minio_client.head_bucket(Bucket=bucket)
        print(f"  Bucket '{bucket}' ya existe.")
    except Exception:
        minio_client.create_bucket(Bucket=bucket)
        print(f"  Bucket '{bucket}' creado.")


# ── PASO 2: Descargar el Parquet a MinIO si no existe ─────────
print("\n[2/5] Verificando archivo Parquet en MinIO...")

try:
    minio_client.head_object(Bucket=BUCKET_RAW, Key=PARQUET_KEY)
    print(f"  Archivo '{PARQUET_KEY}' ya existe en MinIO.")
except Exception:
    print(f"  Descargando {PARQUET_URL} ...")
    response = requests.get(PARQUET_URL, stream=True, timeout=300)
    response.raise_for_status()
    minio_client.upload_fileobj(response.raw, BUCKET_RAW, PARQUET_KEY)
    print(f"  Archivo subido a s3://{BUCKET_RAW}/{PARQUET_KEY}")


# ── PASO 3: Crear SparkSession con Iceberg + Nessie + MinIO ───
print("\n[3/5] Iniciando Spark Session...")

conf = (
    pyspark.SparkConf()
        .setAppName('spark_load_iceberg')
        # JARs ya están en /opt/spark/jars gracias al Dockerfile
        .set('spark.sql.extensions',
             'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
             'org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        # Catálogo Nessie
        .set('spark.sql.catalog.nessie',
             'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri',          CATALOG_URI)
        .set('spark.sql.catalog.nessie.ref',           'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl',
             'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse',     WAREHOUSE)
        .set('spark.sql.catalog.nessie.io-impl',
             'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.sql.catalog.nessie.s3.endpoint',  STORAGE_URI)
        # S3A para leer el Parquet crudo
        .set('spark.hadoop.fs.s3a.endpoint',           STORAGE_URI)
        .set('spark.hadoop.fs.s3a.access.key',         AWS_ACCESS)
        .set('spark.hadoop.fs.s3a.secret.key',         AWS_SECRET)
        .set('spark.hadoop.fs.s3a.path.style.access',  'true')
        .set('spark.hadoop.fs.s3a.aws.credentials.provider',
             'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("  Spark Session iniciada.")


# ── PASO 4: Leer Parquet y crear tabla Iceberg ────────────────
print(f"\n[4/5] Leyendo Parquet desde s3a://{BUCKET_RAW}/{PARQUET_KEY}...")

df_taxis = spark.read.parquet(f"s3a://{BUCKET_RAW}/{PARQUET_KEY}")
total = df_taxis.count()
print(f"  Filas leídas: {total:,}")
df_taxis.printSchema()

print(f"\n[5/5] Escribiendo tabla Iceberg nessie.{NAMESPACE}.{TABLE}...")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{NAMESPACE};")
df_taxis.writeTo(f"nessie.{NAMESPACE}.{TABLE}").createOrReplace()
print(f"  Tabla nessie.{NAMESPACE}.{TABLE} creada/actualizada.")

# Verificación
count = spark.table(f"nessie.{NAMESPACE}.{TABLE}").count()
print(f"  Filas en tabla Iceberg: {count:,}")

spark.stop()
print("\n✅ Pipeline completado exitosamente.")
