#!/bin/bash


cd manager
docker build -t distributed_computing_project-manager:latest .
cd ..

cd worker
docker build -t distributed_computing_project-worker:latest .
cd ..

cd queue_purge
docker build -t distributed_computing_project-queue_purge:latest .
cd ..


