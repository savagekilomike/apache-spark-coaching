#!/bin/bash

ssh -i "${KEY_FILE}" \
  -L 8088:"${PRIMARY_HOST}.${REGION}.compute.amazonaws.com":8088 \
  -L 18080:"${PRIMARY_HOST}.${REGION}.compute.amazonaws.com":18080 \
  -L 8890:"${PRIMARY_HOST}"."${REGION}".compute.amazonaws.com:8890 \
  "${EMR_USER}"@"${PRIMARY_HOST}.${REGION}.compute.amazonaws.com"
