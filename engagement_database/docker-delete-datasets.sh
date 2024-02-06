#!/bin/bash

set -e

# Define the Docker image name and tag
IMAGE_NAME="docker-delete-datasets"


while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN="--dry-run"
            shift;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done


# Check that the correct number of arguments were provided.
if [[ $# -ne 5 ]]; then
    echo "Usage: $0  
    [--dry-run] <google-cloud-credentials-file-path> <engagement-database-credentials-file-url> <database-path> 
    <engagement-db-dataset-1> [<engagement-db-dataset-2> ... <engagement-db-dataset-n>]"
    echo "Deletes messages for given datasets in engagement database"
    exit 1
fi

# Assign the program arguments to bash variables.
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$1
ENGAGEMENT_DATABASE_CREDENTIALS_FILE_URL=$2
DATABASE_PATH=$3
ENGAGEMENT_DB_DATASETS="${@:4}"

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Define the command to run inside the Docker container
CMD="python -u delete_datasets.py ${DRY_RUN} ${GOOGLE_CLOUD_CREDENTIALS_FILE_PATH} ${ENGAGEMENT_DATABASE_CREDENTIALS_FILE_URL} ${DATABASE_PATH} ${ENGAGEMENT_DB_DATASETS}"

# Create a Docker container
container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $GOOGLE_CLOUD_CREDENTIALS_FILE_PATH -> $container_short_id:/path/to/google-cloud-credentials.json"
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/path/to/google-cloud-credentials.json"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Remove the container when it has run successfully
docker container rm "$container" >/dev/null
