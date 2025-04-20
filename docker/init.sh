#!/bin/bash

# Create necessary directories
mkdir -p dags logs plugins config
if [ $? -ne 0 ]; then
  echo "❌ Failed to create directories"
  exit 1
else
  echo "✅ Finished creating structure"
fi

# # Set environment variable
# echo -e "AIRFLOW_UID=$(id -u)" > .env
# if [ $? -ne 0 ]; then
#   echo "❌ Failed to write to .env file"
#   exit 1
# else
#   echo "✅ Finished setting AIRFLOW_UID"
# fi

# Build docker image
docker build -f docker/Dockerfile -t airflow-with-requirements .
if [ $? -ne 0 ]; then
  echo "❌ Docker image build failed"
  exit 1
else
  echo "✅ Finished building docker image"
fi

# Initialize Airflow with docker-compose
docker compose -f ./docker/docker-compose.yaml up airflow-init
if [ $? -ne 0 ]; then
  echo "❌ Airflow initialization failed"
  exit 1
else
  echo "✅ Finished initializing Airflow"
fi

# Prompt for next steps
echo "➡️ Next step is run command: ./docker/start.sh"
