# Shutdown containers gracefully
docker compose -f ./docker/docker-compose.yaml down
if [ $? -ne 0 ]; then
  echo "❌ Failed to bring down containers"
  exit 1
else
  echo "✅ Finished bringing down containers"
fi