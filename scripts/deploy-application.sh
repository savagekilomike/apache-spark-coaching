#!/bin/bash

while getopts 'pl' flag; do
  case "${flag}" in
    p) PACK=TRUE ;;
    l) LOCAL=TRUE ;;
    *) echo "usage: [-p: package environment] [-l: use local environment instead of docker]"
  esac
done

function package_application() {
  cd "${SRC_DIR}" || exit
  zip -r "${BUILD_DIR}/application.zip" "." -x "*__pycache__*"
  cd - || exit
}

function package_environment_local() {
  source "${PROJECT_ROOT}/venv/bin/activate"
  venv-pack -o "${BUILD_DIR}/environment.tar.gz"
  deactivate
}

function package_environment_docker() {
  cd "${PROJECT_ROOT}/scripts" || exit
  cp "${PROJECT_ROOT}/requirements.txt" .
  docker build -f Dockerfile . --output "${BUILD_DIR}"
  rm requirements.txt
  cd - || exit
}

function upload_s3() {
  if [[ $PACK == TRUE && -z ${LOCAL+x} ]]; then
    aws s3 cp "${BUILD_DIR}/environment.tar.gz" "${BUCKET_URL}"
  fi
  aws s3 cp "${BUILD_DIR}/application.zip" "${BUCKET_URL}"
  aws s3 cp --recursive --exclude "*__pycache__*" "${PROJECT_ROOT}/src" "${BUCKET_URL}/src"
  aws s3 cp --recursive "${PROJECT_ROOT}/config" "${BUCKET_URL}/config"
}

function clean() {
  rm "${BUILD_DIR}/*"
}

if [ ! -d "${BUILD_DIR}" ]; then
  mkdir "${BUILD_DIR}"
fi

if [[ $PACK == TRUE ]]; then
  clean
  if [[ $LOCAL == TRUE ]]; then
    package_environment_local
  else
    package_environment_docker
  fi
fi

package_application
upload_s3
