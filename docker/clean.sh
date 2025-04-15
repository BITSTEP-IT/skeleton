# Shutdown containers, remove volumes and images
docker compose -f ./docker/docker-compose.yaml down --volumes --rmi all
if [ $? -ne 0 ]; then
  echo "❌ Failed to bring down containers, remove volumes, or images"
  exit 1
else
  echo "✅ Finished bringing down containers, removed volumes, and images"
fi