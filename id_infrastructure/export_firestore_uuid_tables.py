import argparse
import json

from core_data_modules.logging import Logger

from id_infrastructure.firestore_uuid_table import FirestoreUuidInfrastructure
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exports Firestore uuid table mappings to a json file")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("firebase_credentials_file_url", metavar="firebase-credentials-file-url",
                        help="GS URL of the credentials for the Firestore project to export")
    parser.add_argument("table_names", metavar="table-names", nargs="*",
                        help="Names of the tables to export. If none are provided, exports all tables in Firestore")
    parser.add_argument("export_file_path", metavar="export-file-path",
                        help="JSON file to write the exported tables to.")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    firebase_credentials_file_url = args.firebase_credentials_file_url
    table_names = args.table_names
    export_file_path = args.export_file_path

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        firebase_credentials_file_url
    ))

    id_tables = FirestoreUuidInfrastructure.init_from_credentials(firestore_uuid_table_credentials)
    if len(table_names) == 0:
        table_names = id_tables.list_table_names()
    log.info(f"Found {len(table_names)} uuid tables to export")

    export = dict()  # of table_name -> {mappings: dict of data -> uuid}
    for i, table_name in enumerate(table_names):
        log.info(f"Fetching mappings from table {i + 1}/{len(table_names)}: {table_name}...")
        mappings = id_tables.get_table(table_name, None).get_all_mappings()
        export[table_name] = {
            "mappings": mappings
        }
        log.info(f"Fetched {len(mappings)} mappings")

    log.info(f"Writing mappings to '{export_file_path}'...")
    with open(export_file_path, "w") as f:
        json.dump(export, f)
