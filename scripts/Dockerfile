FROM amazonlinux:latest AS build-stage

WORKDIR /app

RUN yum -y update
RUN yum -y install yum-utils
RUN yum -y groupinstall development

RUN yum list python3*
RUN yum -y install python3 python3-pip python3-virtualenv

ADD requirements.txt .

RUN python3 -m venv venv \
    && source venv/bin/activate \
    && python3 -m pip install -r requirements.txt \
    && venv-pack -o "environment.tar.gz"

FROM scratch AS export-stage
COPY --from=build-stage /app/environment.tar.gz .
