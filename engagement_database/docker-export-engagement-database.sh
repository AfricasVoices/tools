#!/bin/bash

set -e

IMAGE_NAME=docker-export-engagement-database

while [[ $# -gt 0 ]]; do
    case "$1" in
        --incremental-cache-volume)
            INCREMENTAL_ARG="--incremental-cache-path /cache"
            INCREMENTAL_CACHE_VOLUME_NAME="$2"
            shift 2;;
        --gzip-export-file-path)
            GZIP_EXPORT_FILE_PATH_ARG="--gzip-export-file-path /data/output.jsonl.gzip"
            GZIP_EXPORT_FILE_PATH="$2"
            shift 2;;
        --gcs-upload-path)
            GCS_UPLOAD_PATH_ARG="--gcs-upload-path $2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -ne 3 ]]; then
    echo "Usage: $0 
    [--incremental-cache-volume <incremental-cache-volume>]
    [--gzip-export-file-path <gzip-export-file-path>]
    [--gcs-upload-path <gcs-upload-path>]
    <google-cloud-credentials-file-path> <engagement-database-credentials-file-url> <database-path>"
    exit
fi

# Assign the program arguments to bash variables.
GOOGLE_CLOUD_CREDENTIALS_PATH=$1
ENGAGEMENT_DATABASE_CREDENTIALS_FILE_URL=$2
DATABASE_PATH=$3

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u export_engagement_database.py ${INCREMENTAL_ARG} \
    ${GZIP_EXPORT_FILE_PATH_ARG} ${GCS_UPLOAD_PATH_ARG} \
    /credentials/google-cloud-credentials.json ${ENGAGEMENT_DATABASE_CREDENTIALS_FILE_URL} ${DATABASE_PATH}"

if [[ "$INCREMENTAL_ARG" ]]; then
    container="$(docker container create -w /app --mount source="$INCREMENTAL_CACHE_VOLUME_NAME",target=/cache "$IMAGE_NAME" /bin/bash -c "$CMD")"
else
    container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
fi

echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $GOOGLE_CLOUD_CREDENTIALS_PATH -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$container:/credentials/google-cloud-credentials.json"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# If we're in file export mode, copy the file out of the container
if [[ "$GZIP_EXPORT_FILE_PATH" ]]; then
    echo "Copying $container_short_id:/data/output.jsonl.gzip -> $GZIP_EXPORT_FILE_PATH"
    docker cp "$container:/data/output.jsonl.gzip" "$GZIP_EXPORT_FILE_PATH"
fi

# Tear down the container when it has run successfully
docker container rm "$container" >/dev/null
