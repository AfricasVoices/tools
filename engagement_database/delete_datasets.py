import argparse
import json

from core_data_modules.logging import Logger
from google.cloud import firestore
from storage.google_cloud import google_cloud_utils
from engagement_database import EngagementDatabase

log = Logger(__name__)

BATCH_SIZE = 500


@firestore.transactional
def delete_message_and_history(transaction, message_id):
    engagement_db.delete_message_and_history(message_id, transaction=transaction)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deletes messages for given datasets in engagement database")

    parser.add_argument("--dry-run", action="store_true",
                        help="Logs the updates that would be made without updating anything.")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("engagement_database_credentials_file_url", metavar="engagement-database-credentials-file-url",
                        help="GS URL to the Engagement firestore database credentials file")
    parser.add_argument("database_path", metavar="database-path",
                        help="Path to the engagement database to delete messages and history from e.g. engagement_databases/test")
    parser.add_argument("engagement_db_datasets", nargs="+", metavar="engagement-db-datasets",
                        help="Engagement database datasets to delete")

    args = parser.parse_args()

    dry_run = args.dry_run
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    engagement_database_credentials_file_url = args.engagement_database_credentials_file_url
    database_path = args.database_path
    engagement_db_datasets = args.engagement_db_datasets

    dry_run_text = "(dry run)" if dry_run else ""
    log.info(f"Deleting engagement database messages in {len(engagement_db_datasets)} dataset(s) {dry_run_text}")

    log.info("Downloading Firestore engagement database credentials...")
    engagement_database_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        engagement_database_credentials_file_url
    ))

    engagement_db = EngagementDatabase.init_from_credentials(engagement_database_credentials, database_path)
    transaction = engagement_db.transaction()

    for engagement_db_dataset in engagement_db_datasets:
        messages_filter = lambda q: q.where("dataset", "==", engagement_db_dataset)
        messages = engagement_db.get_messages(firestore_query_filter=messages_filter, batch_size=BATCH_SIZE)
        log.info(f"Downloaded {len(messages)} messages from dataset {engagement_db_dataset}")

        for count, msg in enumerate(messages, start=1):
            log.info(f"Deleting engagement db message {count}/{len(messages)} with id {msg.message_id} and its history entries")
            if not dry_run:
                delete_message_and_history(transaction, msg.message_id)
