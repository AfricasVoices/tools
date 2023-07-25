import argparse
import json
import subprocess

from core_data_modules.logging import Logger
from engagement_database import EngagementDatabase
from engagement_database.data_models import HistoryEntryOrigin, CommandLogEntry, CommandStatuses
from google.cloud import firestore
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

BATCH_SIZE = 500


@firestore.transactional
def reset_message_labels(transaction, engagement_db, message_id, dry_run=False):
    message = engagement_db.get_message(message_id, transaction)
    assert message is not None, f"Message '{message_id}' does not exist in database"

    # Reset the message's labels and previous_datasets, and return the message to its original dataset
    message.labels = []
    message.dataset = message.previous_datasets[0]
    message.previous_datasets = []

    if not dry_run:
        engagement_db.set_message(
            message,
            HistoryEntryOrigin("Reset message labels", {}),
            transaction
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resets all of a message's labels and previous_datasets, and returns "
                                                 "the message back to its original dataset")

    parser.add_argument("--dry-run", const=True, default=False, action="store_const")
    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("engagement_database_credentials_file_url", metavar="engagement-database-credentials-file-url",
                        help="GS URL of the credentials for the Firestore project to export")
    parser.add_argument("database_path", metavar="database-path",
                        help="Path to the engagement database to export e.g. engagement_databases/test")
    parser.add_argument("message_id", metavar="message-id",
                        help="Id of message to reset labels for")

    args = parser.parse_args()

    dry_run = args.dry_run
    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    engagement_database_credentials_file_url = args.engagement_database_credentials_file_url
    database_path = args.database_path
    message_id = args.message_id

    dry_run_text = " (dry run)" if dry_run else ""

    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
    HistoryEntryOrigin.set_defaults(user, project, "NA", commit)

    log.info("Downloading Firestore engagement database credentials...")
    engagement_database_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        engagement_database_credentials_file_url
    ))
    engagement_db = EngagementDatabase.init_from_credentials(engagement_database_credentials, database_path)
    log.info(f"Initialised the Engagement Database client")

    if not dry_run:
        engagement_db.set_command_log_entry(CommandLogEntry(status=CommandStatuses.STARTED))

    log.info(f"Resetting labels for message '{message_id}'{dry_run_text}...")
    transaction = engagement_db.transaction()
    reset_message_labels(transaction, engagement_db, message_id, dry_run)

    if not dry_run:
        engagement_db.set_command_log_entry(CommandLogEntry(status=CommandStatuses.COMPLETED_SUCCESSFULLY))

    log.info(f"Done. Reset labels for message '{message_id}'{dry_run_text}")
