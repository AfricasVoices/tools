import argparse
import json

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils
from engagement_database import EngagementDatabase

log = Logger(__name__)

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
                        help="Path to the engagement database e.g. engagement_databases/test")
    parser.add_argument("engagement_db_datasets", nargs="+", metavar="engagement-db-datasets",
                        help="Engagement database datasets to delete")

    args = parser.parse_args()

    dry_run = args.dry_run
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    engagement_database_credentials_file_url = args.engagement_database_credentials_file_url
    database_path = args.database_path
    engagement_db_datasets = args.engagement_db_datasets

    dry_run_text = "(dry run)" if dry_run else ""
    log.info(f"Deletes messages in given engagement database {dry_run_text}")

    log.info("Downloading Firestore engagement database credentials...")
    engagement_database_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        engagement_database_credentials_file_url
    ))

    engagement_db = EngagementDatabase.init_from_credentials(engagement_database_credentials, database_path)

    for engagement_db_dataset in engagement_db_datasets:
        messages_filter = lambda q: q.where("dataset", "==", engagement_db_dataset)
        messages = engagement_db.get_messages(firestore_query_filter=messages_filter, batch_size=500)

        for msg in messages:
            log.warning(f"Deleting engagement db message with message id {msg.message_id}")
            if not dry_run:
                engagement_db.delete_doc(f"messages/{msg.message_id}")

            history_entries = engagement_db.get_history_for_message(msg.message_id)
            log.warning(f"Found {len(history_entries)} engagement db message's history entries")
            for history_entry in history_entries:
                log.warning(f"Deleting history entry with id {history_entry.history_entry_id}")
                if not dry_run:
                    engagement_db.delete_doc(f"history/{history_entry.history_entry_id}")
