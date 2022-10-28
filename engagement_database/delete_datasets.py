import argparse
import json

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from engagement_database import EngagementDatabase

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deletes messages for given datasets in engagement database")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("engagement_database_credentials_file_url", metavar="engagement-database-credentials-file-url",
                        help="GS URL of the credentials for the Firestore project")
    parser.add_argument("database_path", metavar="database-path",
                        help="Path to the engagement database e.g. engagement_databases/test")
    parser.add_argument("engagement_db_datasets", nargs="+", metavar="engagement-db-datasets",
                        help="List of engagement database datasets to delete")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    engagement_database_credentials_file_url = args.engagement_database_credentials_file_url
    database_path = args.database_path
    engagement_db_datasets = args.engagement_db_datasets

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
            history_entries = engagement_db.get_history_for_message(msg.message_id)
            for history_entry in history_entries:
                engagement_db.delete_doc(f"history/{history_entry.history_entry_id}")

            engagement_db.delete_doc(f"messages/{msg.message_id}")
