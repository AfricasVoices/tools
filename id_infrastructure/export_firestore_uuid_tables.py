import argparse
import gzip
import json

from core_data_modules.logging import Logger

from id_infrastructure.firestore_uuid_table import FirestoreUuidInfrastructure
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exports Firestore uuid table mappings to a zipped json file")

    parser.add_argument("--gzip-export-file-path",
                        help="json.gzip file to write the exported data to")
    parser.add_argument("--gcs-upload-path",
                        help="GS URL to upload the exported json.gzip to")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("firebase_credentials_file_url", metavar="firebase-credentials-file-url",
                        help="GS URL of the credentials for the Firestore project to export")
    parser.add_argument("table_names", metavar="table-names", nargs="*",
                        help="Names of the tables to export. If none are provided, exports all tables in Firestore")

    args = parser.parse_args()

    gzip_export_file_path = args.gzip_export_file_path
    gcs_upload_path = args.gcs_upload_path
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    firebase_credentials_file_url = args.firebase_credentials_file_url
    table_names = args.table_names

    if gzip_export_file_path is None and gcs_upload_path is None:
        log.error(f"No output locations specified. Please provide at least one of --gzip-export-file-path or "
                  f"--gcs-upload-path")
        exit(1)

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

    log.info(f"Converting fetched data to zipped json for export...")
    json_blob = json.dumps(export)
    export_compressed = gzip.compress(bytes(json_blob, "utf-8"))

    if gzip_export_file_path is not None:
        log.warning(f"Writing mappings to local disk at '{gzip_export_file_path}'...")
        with open(gzip_export_file_path, "wb") as f:
            f.write(export_compressed)

    if gcs_upload_path is not None:
        log.info(f"Uploading the mappings to {gcs_upload_path}...")
        google_cloud_utils.upload_string_to_blob(google_cloud_credentials_file_path, gcs_upload_path,
                                                 export_compressed)
    log.info("Done")
