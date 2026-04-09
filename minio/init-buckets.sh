set -e

echo "Waiting for MinIO to be ready..."
until mc alias set myminio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} 2>/dev/null; do
  echo "MinIO is not ready yet. Retrying in 3 seconds..."
  sleep 3
done

echo "MinIO is ready. Creating buckets..."

mc mb --ignore-existing myminio/bronze
echo "Bronze bucket created"

mc mb --ignore-existing myminio/silver
echo "Silver bucket created"

mc mb --ignore-existing myminio/gold
echo "Gold bucket created"

mc mb --ignore-existing myminio/landing
echo "Landing bucket created (for CSV uploads)"

mc mb --ignore-existing myminio/warehouse
echo "Warehouse bucket created (for Trino metastore)"

echo "All buckets created and configured successfully!"
echo "Buckets available:"
mc ls myminio/
