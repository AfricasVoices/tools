#!/bin/bash

set -e

if [[ $# -ne 7 ]]; then
    echo "Usage: $0
    <incremental-cache-volume> <google-cloud-credentials-file-path> <gcs-upload-prefix>
    <engagement-database-credentials-file-url> <database-path>
    <full-backup-interval-seconds> <incremental-backup-interval-seconds>
    "
    exit
fi

INCREMENTAL_CACHE_VOLUME=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
GCS_UPLOAD_PREFIX=$3
ENGAGEMENT_DATABASE_CREDENTIALS_FILE_URL=$4
DATABASE_PATH=$5
FULL_BACKUP_INTERVAL=$6
INCREMENTAL_BACKUP_INTERVAL=$7

# Make sure there isn't a cache volume with this name already, so that the first backup runs correctly
if docker volume inspect "$INCREMENTAL_CACHE_VOLUME" >/dev/null 2>&1; then
    echo "Error: Volume '$INCREMENTAL_CACHE_VOLUME' already exists. Please remove it and re-run"
    exit
fi

BACKUP_ID=$(uuidgen)
LAST_FULL_BACKUP_SECONDS=0

while true; do
    # Check if enough time has passed since the last full backup, and set-up for a new full backup if it has.
    if [ $(($(date -u +%s) - LAST_FULL_BACKUP_SECONDS)) -gt "$FULL_BACKUP_INTERVAL" ]; then
        echo "Preparing new full backup"
        # Remove the incremental cache volume if it exists.
        docker volume inspect "$INCREMENTAL_CACHE_VOLUME" >/dev/null 2>&1 && docker volume rm "$INCREMENTAL_CACHE_VOLUME"

        # Assign a new random id for this full backup, and record the start date using the time now.
        BACKUP_ID=$(uuidgen)
        LAST_FULL_BACKUP_SECONDS="$(date -u +%s)"
        LAST_FULL_BACKUP_STR=$(date -r "$LAST_FULL_BACKUP_SECONDS" +'%Y_%m_%d__%H_%M_%S_Z')
        INCREMENTAL_BACKUP_STR=$LAST_FULL_BACKUP_STR
    else
        echo "Preparing incremental backup"
        INCREMENTAL_BACKUP_STR=$(date -u +'%Y_%m_%d__%H_%M_%S_Z')
    fi

    # Run the backup. Upload to a filename that includes
    # (i) the full backup timestamp,
    # (ii) incremental backup timestamp
    # (iii) random backup id for the full backup
    # This ensures we can tell which backup files are linked to the same backup.
    echo "Backing up to '$GCS_UPLOAD_PREFIX-full-$LAST_FULL_BACKUP_STR-id-$BACKUP_ID-incremental-$INCREMENTAL_BACKUP_STR.jsonl.gzip'..."
    ./docker-export-engagement-database.sh \
        --incremental-cache-volume "$INCREMENTAL_CACHE_VOLUME" \
        --gcs-upload-path "$GCS_UPLOAD_PREFIX-full-$LAST_FULL_BACKUP_STR-id-$BACKUP_ID-incremental-$INCREMENTAL_BACKUP_STR.jsonl.gzip" \
        "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$ENGAGEMENT_DATABASE_CREDENTIALS_FILE_URL" "$DATABASE_PATH"

    # Sleep until the next incremental backup is due
    echo "Backup complete at $(date -u). Last full backup was started at $(date -r "$LAST_FULL_BACKUP_SECONDS")"
    echo "Sleeping for $INCREMENTAL_BACKUP_INTERVAL seconds"
    sleep "$INCREMENTAL_BACKUP_INTERVAL"
done
