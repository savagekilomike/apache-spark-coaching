#!/bin/bash

NAME="hello-world"
PY_FILES="${BUCKET_URL}/application.zip"
FILES="${BUCKET_URL}/config/conf-aws.json"
ARCHIVES="${BUCKET_URL}/environment.tar.gz#environment"
MAIN="${BUCKET_URL}/src/analyze.py"
ARGS="conf-aws.json"

aws emr add-steps --region "${REGION}" --cluster-id "${CLUSTER_ID}" \
  --steps '[
    {
      "Args":[
        "spark-submit",
        "--deploy-mode","cluster",
        "--archives","'${ARCHIVES}'",
        "--py-files","'${PY_FILES}'",
        "--files","'${FILES}'",
        "--conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python",
        "--conf", "spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python",
        "'${MAIN}'",
        "'${ARGS}'"
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"CONTINUE",
      "Jar":"command-runner.jar",
      "Name":"'${NAME}'"
    }
  ]'
