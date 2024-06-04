#!/bin/bash

# ------------------------------------------
# RUN THESE COMMANDS ON THE EMR PRIMARY NODE
# ------------------------------------------

# Set bucket name to environment
export BUCKET_URL="s3://<set-bucket-name>"

# Submit application with archive
PYSPARK_PYTHON=./environment/bin/python \
spark-submit \
  --deploy-mode cluster \
  --py-files "${BUCKET_URL}/application.zip" \
  --files "${BUCKET_URL}/config/conf-aws.json" \
  --archives "${BUCKET_URL}/environment.tar.gz#environment" \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python \
  "${BUCKET_URL}/src/prepare_step.py" "conf-aws.json"

# View logs for application

yarn logs -applicationId <appId> -log_files stdout