import argparse
import csv
import json
import subprocess
import sys
from collections import defaultdict
from io import StringIO

from core_data_modules.logging import Logger
from dateutil.parser import isoparse
from engagement_database import EngagementDatabase
from engagement_database.data_models import (HistoryEntryOrigin, Message, MessageStatuses, CommandLogEntry,
                                             CommandStatuses)
from google.cloud import firestore
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Archives messages that are the best matches for each of the messages in the referenced dataset. \n"
                    "For example, to archive duplicated messages, provide a dataset of known duplicates identified "
                    "by an external script that searched for identical messages sent very close to each other in time."
    )

    parser.add_argument("--dry-run", const=True, default=False, action="store_const")
    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("engagement_database_credentials_file_url", metavar="engagement-database-credentials-file-url",
                        help="GS URL to the Firestore credentials file")
    parser.add_argument("database_path", metavar="database-path",
                        help="Path to the engagement database e.g. engagement_databases/test")
    parser.add_argument("messages_to_archive_csv_url", metavar="messages-to-archive-csv-url",
                        help="URL to a csv containing messages to archive. For each message in this CSV, the closest "
                             "message in time in the database which has the same participant_uuid and text will be "
                             "archived. This CSV must have the headers 'avf-participant-uuid', 'text', and 'timestamp'")

    args = parser.parse_args()

    dry_run = args.dry_run
    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    engagement_database_credentials_file_url = args.engagement_database_credentials_file_url
    database_path = args.database_path
    messages_to_archive_csv_url = args.messages_to_archive_csv_url

    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
    HistoryEntryOrigin.set_defaults(user, project, "NA", commit)

    dry_run_text = ' (dry run)' if dry_run else ''

    log.info("Downloading engagement database credentials...")
    engagement_database_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        engagement_database_credentials_file_url
    ))
    engagement_db = EngagementDatabase.init_from_credentials(engagement_database_credentials, database_path)

    if not dry_run:
        engagement_db.set_command_log_entry(CommandLogEntry(status=CommandStatuses.STARTED))

    log.info(f"Loading messages to be archived from {messages_to_archive_csv_url}...")
    messages_to_archive_csv = \
        google_cloud_utils.download_blob_to_string(google_cloud_credentials_file_path, messages_to_archive_csv_url)
    messages_to_archive = list(csv.DictReader(StringIO(messages_to_archive_csv)))
    log.info(f"Downloaded {len(messages_to_archive)} messages to archive")

    log.info(f"Archiving {len(messages_to_archive)} messages{dry_run_text}...")
    matched_message_ids = set()
    for i, msg_to_archive in enumerate(messages_to_archive):
        # Get the possible matching messages from the database
        possible_matching_messages = engagement_db.get_messages(
            firestore_query_filter=lambda q: q
                .where("text", "==", msg_to_archive["text"])
                .where("participant_uuid", "==", msg_to_archive["avf-participant-uuid"])
        )

        if len(possible_matching_messages) == 0 and msg_to_archive["text"] == "":
            possible_matching_messages = engagement_db.get_messages(
                firestore_query_filter=lambda q: q
                    .where("text", "==", None)
                    .where("participant_uuid", "==", msg_to_archive["avf-participant-uuid"])
            )

        # Make sure we don't match a message we already saw
        possible_matching_messages = [msg for msg in possible_matching_messages if msg.message_id not in matched_message_ids]
        log.info(f"Found {len(possible_matching_messages)} possible matching messages for message {i + 1}")

        # Find the nearest message in time to this duplicate
        timestamp_of_duplicate = isoparse(msg_to_archive["timestamp"])
        possible_matching_messages.sort(key=lambda msg: abs((msg.timestamp - timestamp_of_duplicate).total_seconds()))

        nearest_match = possible_matching_messages[0]
        log.info(f"Found best matching message with message_id '{nearest_match.message_id}' for message {i + 1}. "
                 f"Timedelta is {nearest_match.timestamp - timestamp_of_duplicate}")

        # Archive the message in the database
        if nearest_match.status == MessageStatuses.ARCHIVED:
            log.warning(f"Message {nearest_match.message_id} already has status {MessageStatuses.ARCHIVED}")

        nearest_match.status = MessageStatuses.ARCHIVED
        origin = HistoryEntryOrigin("Archive Duplicate Message", {"duplicate": msg_to_archive})
        engagement_db.set_message(nearest_match, origin)

    if not dry_run:
        engagement_db.set_command_log_entry(CommandLogEntry(status=CommandStatuses.COMPLETED_SUCCESSFULLY))
    log.info(f"Archived {len(messages_to_archive)} messages{dry_run_text}.")
