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
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -ne 5 ]]; then
    echo "Usage: ./docker-run.sh
    <google-cloud-credentials-file-path> <instance-1-domain> <instance-1-credentials-url>
    <instance-2-domain> <instance-2-credentials-url>"
    exit
fi

# Assign the program arguments to bash variables.
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$1
INSTANCE_1_DOMAIN=$2
INSTANCE_1_CREDENTIALS_URL=$3
INSTANCE_2_DOMAIN=$4
INSTANCE_2_CREDENTIALS_URL=$5

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

CMD="pipenv run python -u synchronise_contacts.py $FORCE /credentials/google-cloud-credentials.json \
     \"$INSTANCE_1_DOMAIN\" \"$INSTANCE_1_CREDENTIALS_URL\" \"$INSTANCE_2_DOMAIN\" \"$INSTANCE_2_CREDENTIALS_URL\"
"
container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/credentials/google-cloud-credentials.json"

# Run the container
docker start -a -i "$container"
