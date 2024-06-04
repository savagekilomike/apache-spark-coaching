#!/bin/bash

export PROJECT_ROOT=""
export BUILD_DIR="${PROJECT_ROOT}/build"
export SRC_DIR="${PROJECT_ROOT}/src"
export CONFIG_DIR="${PROJECT_ROOT}/config"

export BUCKET_NAME="com.example.bucket-name"
export BUCKET_URL="s3://${BUCKET_NAME}"
export REGION=""
export CLUSTER_ID=""
export KEY_FILE="${PROJECT_ROOT}/scripts/<key-file>.pem"
export PRIMARY_HOST=""
export EMR_USER="hadoop"
