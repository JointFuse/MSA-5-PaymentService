#!/bin/bash

export CAMUNDA_PLATFORM_VERSION=8.5.22
export CAMUNDA_OPERATE_VERSION=8.5.22
export CAMUNDA_TASKLIST_VERSION=8.5.24
export ELASTIC_VERSION=8.14.3

docker-compose down -v
docker-compose pull
docker-compose build
docker-compose up -d