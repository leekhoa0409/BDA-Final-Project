#!/bin/bash
set -e

# Create writable config directory and substitute environment variables
mkdir -p /tmp/spark-conf
envsubst < /opt/spark/conf/spark-defaults.conf.template > /tmp/spark-conf/spark-defaults.conf

# Copy any other existing conf files
for f in /opt/spark/conf/*; do
    [ "$(basename "$f")" = "spark-defaults.conf.template" ] && continue
    [ "$(basename "$f")" = "spark-defaults.conf" ] && continue
    cp "$f" /tmp/spark-conf/ 2>/dev/null || true
done

export SPARK_CONF_DIR=/tmp/spark-conf

# Execute the original command
exec "$@"
