# Start containers in detached mode
docker compose -f ./docker/docker-compose.yaml up -d
if [ $? -ne 0 ]; then
  echo "❌ Failed to start containers in detached mode"
  exit 1
else
  echo "✅ Finished starting containers in detached mode"
fi

# Prompt for next steps
echo "➡️ Next step is run command: ./docker/start.sh"