# Databricks notebook source
# DBTITLE 1,Pipeline Overview
# MAGIC %md
# MAGIC ##1. Data Ingestion
# MAGIC
# MAGIC •	Load both datasets using Python (Pandas or PySpark).
# MAGIC
# MAGIC •	Write a script that ingests data into a staging layer (can be local file system or S3-like structure).
# MAGIC
# MAGIC •	Store raw → cleaned processed version separately.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Config & Logger
import fnmatch
import logging
from datetime import datetime
from pyspark.sql import functions as F

logger = logging.getLogger("data_ingestion")
logger.setLevel(logging.INFO)
for _h in logger.handlers[:]:
    logger.removeHandler(_h)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(_handler)

LANDING_DIR = "/Volumes/assignment_1/bronze/raw_data/landing/"
PROCESSED_DIR = "/Volumes/assignment_1/bronze/raw_data/processed/"
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

DATASETS = [
    {"name": "orders", "file_pattern": "orders*.csv", "target_table": "assignment_1.bronze.orders"},
    {"name": "users",  "file_pattern": "users*.csv",  "target_table": "assignment_1.bronze.users"},
]

logger.info("Datasets to load: %s", ", ".join(d["name"] for d in DATASETS))

# COMMAND ----------

# DBTITLE 1,Check Landing Folder
# Check if landing folder has any data files before proceeding
import fnmatch

all_files = []
try:
    all_files = dbutils.fs.ls(LANDING_DIR)
except Exception:
    pass

matching_files = []
for d in DATASETS:
    matching_files.extend([f for f in all_files if fnmatch.fnmatch(f.name, d["file_pattern"])])

if not matching_files:
    logger.warning("Landing folder '%s' is empty. No data files found for ingestion.", LANDING_DIR)
    dbutils.jobs.taskValues.set(key="landing_folder_empty", value="true")
    dbutils.notebook.exit("Landing folder is empty. No data files found for ingestion. Skipping downstream tasks (clean_and_transformation and sql_analytics).")
else:
    logger.info("Found %d file(s) in landing folder. Proceeding with ingestion.", len(matching_files))
    dbutils.jobs.taskValues.set(key="landing_folder_empty", value="false")

# COMMAND ----------

# DBTITLE 1,Ingest Dataset
def ingest(name, file_pattern, target_table):
    """Read one landing dataset into its bronze table, then archive the file(s)."""
    try:
        files = [f for f in dbutils.fs.ls(LANDING_DIR) if fnmatch.fnmatch(f.name, file_pattern)]
        if not files:
            logger.warning(f"[{name}] no file found for '{file_pattern}' -- skipped")
            return

        # 2. Read the CSV(s) into the bronze table
        df = spark.read.csv(LANDING_DIR + file_pattern, header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        logger.info(f"[{name}] loaded {spark.table(target_table).count()} rows into {target_table}")

        # 3. Move the file(s) to the processed folder (one date_time folder per run)
        processed_path = f"{PROCESSED_DIR}{RUN_ID}"
        for file in files:
            dbutils.fs.mv(
                file.path,
                f"{processed_path}/{file.name}",
                True,
            )
        logger.info(f"[{name}] archived {len(files)} file(s) to {processed_path}")

    except Exception as e:
        logger.error(f"[{name}] FAILED: {type(e).__name__}: {e}")

# COMMAND ----------

# DBTITLE 1,Run All Datasets
for d in DATASETS:
    logger.info(f"=== {d['name']} ===")
    ingest(d["name"], d["file_pattern"], d["target_table"])

# COMMAND ----------

# DBTITLE 1,Validation
# Quick check: row counts in the bronze tables and files in the processed folder
for d in DATASETS:
    logger.info(f"{d['target_table']}: {spark.table(d['target_table']).count()} rows")

#Move the files from /landing to /processed
for run_dir in sorted(f.name for f in dbutils.fs.ls(PROCESSED_DIR)):
    archived = [f.name for f in dbutils.fs.ls(f"{PROCESSED_DIR}{run_dir}/")]
    logger.info(f"Archived in {run_dir}: {archived}")