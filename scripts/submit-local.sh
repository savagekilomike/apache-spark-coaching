#!/bin/bash

SPARK_HOME="${PROJECT_ROOT}/spark-3.5.0-bin-hadoop3"

source "${PROJECT_ROOT}"/venv/bin/activate

"${SPARK_HOME}"/bin/spark-submit \
  --deploy-mode client \
  --master local[*] \
  "${SRC_DIR}/prepare.py" "${CONFIG_DIR}/conf.json"

deactivate
