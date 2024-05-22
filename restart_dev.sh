#!/bin/sh

docker-compose -f docker-compose-dev.yml down
docker container ls -a | grep dis-responder-app | awk '{print $1}' | xargs docker container rm
docker image ls | grep dis-responder-app | awk '{print $3}' | xargs docker image rm
docker volume rm dis-responder_static_volume
docker-compose -f docker-compose-dev.yml up -d
