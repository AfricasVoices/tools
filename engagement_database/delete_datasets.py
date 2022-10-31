import argparse
import json

from coda_v2_python_client.firebase_client_wrapper import CodaV2Client
from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils
from engagement_database import EngagementDatabase

log = Logger(__name__)

class keyvalue(argparse.Action):
    """
    argparse action to split an argument into KEY=VALUE form
    on the = and append to a dictionary.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())
          
        for value in values:
            key, value = value.split('=')
            getattr(namespace, self.dest)[key] = value


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
    coda_credentials_file_url = args.coda_credentials_file_url
    engagement_db_to_coda_dataset = args.engagement_db_to_coda_dataset

    log.info("Downloading Firestore engagement database credentials...")
    engagement_database_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        engagement_database_credentials_file_url
    ))

    log.info("Downloading Firestore coda credentials...")
    coda_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        coda_credentials_file_url
    ))

    engagement_db = EngagementDatabase.init_from_credentials(engagement_database_credentials, database_path)
    coda = CodaV2Client.init_client(coda_credentials)

    for engagement_db_dataset, coda_dataset in engagement_db_to_coda_dataset.items():
        messages_filter = lambda q: q.where("dataset", "==", engagement_db_dataset)
        messages = engagement_db.get_messages(firestore_query_filter=messages_filter, batch_size=500)

        for msg in messages:
            if msg.coda_id is not None:
                coda_message = coda.get_dataset_message(coda_dataset, msg.coda_id)
                assert coda_message is not None

            engagement_db.delete_doc(f"messages/{msg.message_id}")

            history_entries = engagement_db.get_history_for_message(msg.message_id)
            for history_entry in history_entries:
                engagement_db.delete_doc(f"history/{history_entry.history_entry_id}")

            coda.delete_dataset_message(coda_dataset, msg.coda_id)
