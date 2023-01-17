#!/bin/bash

set -e

IMAGE_NAME=export-firestore-uuid-tables

while [[ $# -gt 0 ]]; do
    case "$1" in
        --gzip-export-file-path)
            GZIP_EXPORT_FILE_PATH_ARG="--gzip-export-file-path /data/export.json.gzip"
            GZIP_EXPORT_FILE_PATH=$2
            shift
            shift;;
        --gcs-upload-path)
            GCS_UPLOAD_PATH_ARG="--gcs-upload-path \"$2\""
            shift
            shift;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -lt 2 ]]; then
    echo "Usage: ./docker-run-export-firestore-uuid-tables.sh [--gzip-export-file-path <path>] [--gcs-upload-path <path>]
    <google-cloud-credentials-file-path> <firebase-credentials-file-url> [<table-name-1> ... <table-name-n>]"
    exit
fi

# Assign the program arguments to bash variables.
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$1
FIREBASE_CREDENTIALS_FILE_URL=$2
TABLE_NAMES=${*:3}

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

CMD="pipenv run python -u export_firestore_uuid_tables.py $GZIP_EXPORT_FILE_PATH_ARG $GCS_UPLOAD_PATH_ARG \
     /credentials/google-cloud-credentials.json \"$FIREBASE_CREDENTIALS_FILE_URL\" $TABLE_NAMES
"
container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $GOOGLE_CLOUD_CREDENTIALS_FILE_PATH -> $container:/credentials/google-cloud-credentials.json"
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/credentials/google-cloud-credentials.json"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Copy the output data back out of the container
if [ -n "$GZIP_EXPORT_FILE_PATH" ]; then
    echo "Copying $container_short_id:/data/export.json.gzip -> $GZIP_EXPORT_FILE_PATH"
    docker cp "$container:/data/export.json.gzip" "$GZIP_EXPORT_FILE_PATH"
fi

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
