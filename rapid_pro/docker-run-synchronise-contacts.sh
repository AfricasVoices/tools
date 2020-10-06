#!/bin/bash

set -e

IMAGE_NAME=synchronise-contacts

while [[ $# -gt 0 ]]; do
    case "$1" in
        -f)
            FORCE="-f"
            shift;;
        --force)
            FORCE="--force"
            shift;;
        --dry-run)
            DRY_RUN="--dry-run"
            shift;;
        --update)
            UPDATE="--update $2"
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
if [[ $# -ne 6 ]]; then
    echo "Usage: ./docker-run.sh [--force | -f] [--dry-run] [--update {1, 2, both}]
    <google-cloud-credentials-file-path> <workspace-1-domain> <workspace-1-credentials-url>
    <workspace-2-domain> <workspace-2-credentials-url> <raw-data-log-directory>"
    exit
fi

# Assign the program arguments to bash variables.
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$1
WORKSPACE_1_DOMAIN=$2
WORKSPACE_1_CREDENTIALS_URL=$3
WORKSPACE_2_DOMAIN=$4
WORKSPACE_2_CREDENTIALS_URL=$5
RAW_DATA_LOG_DIRECTORY=$6

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

CMD="pipenv run python -u synchronise_contacts.py $FORCE $DRY_RUN $UPDATE /credentials/google-cloud-credentials.json \
     \"$WORKSPACE_1_DOMAIN\" \"$WORKSPACE_1_CREDENTIALS_URL\" \"$WORKSPACE_2_DOMAIN\" \"$WORKSPACE_2_CREDENTIALS_URL\" \
     /data/raw-data-logs
"
container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/credentials/google-cloud-credentials.json"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Copy the output data back out of the container
echo "Copying $container_short_id:/data/raw-data-logs/. -> $RAW_DATA_LOG_DIRECTORY"
docker cp "$container:/data/raw-data-logs/." "$RAW_DATA_LOG_DIRECTORY"

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
