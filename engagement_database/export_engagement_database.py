import argparse
import gzip
import json

from core_data_modules.logging import Logger
from engagement_database import EngagementDatabase

from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exports an engagement database to a zipped json file")

    parser.add_argument("--gzip-export-file-path",
                        help="json.gzip file to write the exported data to")
    parser.add_argument("--gcs-upload-path",
                        help="GS URL to upload the exported json.gzip to")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("engagement_database_credentials_file_url", metavar="engagement-database-credentials-file-url",
                        help="GS URL of the credentials for the Firestore project to export")
    parser.add_argument("database_path", metavar="database-path",
                        help="Path to the engagement database to export")

    args = parser.parse_args()

    gzip_export_file_path = args.gzip_export_file_path
    gcs_upload_path = args.gcs_upload_path
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    engagement_database_credentials_file_url = args.engagement_database_credentials_file_url
    database_path = args.database_path

    if gzip_export_file_path is None and gcs_upload_path is None:
        log.error(f"No output locations specified. Please provide at least one of --gzip-export-file-path or "
                  f"--gcs-upload-path")
        exit(1)

    log.info("Downloading Firestore UUID Table credentials...")
    engagement_database_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        engagement_database_credentials_file_url
    ))

    engagement_db = EngagementDatabase.init_from_credentials(engagement_database_credentials, database_path)
    messages = engagement_db.get_messages()

    export = []
    for msg in messages:
        export.append({"type": "message", "data": msg.to_dict(serialize_datetimes_to_str=True)})

    log.info(f"Converting fetched data to zipped jsonl for export...")
    jsonl_blob = ""
    for item in export:
        jsonl_blob += json.dumps(item) + "\n"
    export_compressed = gzip.compress(bytes(jsonl_blob, "utf-8"))

    if gzip_export_file_path is not None:
        log.warning(f"Writing export to local disk at '{gzip_export_file_path}'...")
        with open(gzip_export_file_path, "wb") as f:
            f.write(export_compressed)
