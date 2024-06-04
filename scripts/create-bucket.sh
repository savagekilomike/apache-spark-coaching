#!/bin/bash

aws s3api create-bucket \
    --bucket "${BUCKET_NAME}" \
    --region "${REGION}"
