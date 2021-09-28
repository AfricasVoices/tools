import argparse
import gzip
import json
import shutil
import tempfile

from core_data_modules.logging import Logger
from engagement_database import EngagementDatabase
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

BATCH_SIZE = 500

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

    with tempfile.TemporaryDirectory() as dir_path:
        raw_export_path = f"{dir_path}/export.jsonl"
        compressed_export_path = f"{dir_path}/export.jsonl.gzip"

        log.info(f"Exporting data to an uncompressed, temporary file at '{raw_export_path}'...")
        with open(raw_export_path, "w") as f:
            log.info(f"Exporting all messages...")
            # Paginate the export because Firestore returns incomplete results when making queries that have a long run time
            total_messages = 0
            batch_messages = engagement_db.get_messages(firestore_query_filter=lambda q: q.order_by("message_id").limit(BATCH_SIZE))
            while len(batch_messages) > 0:
                # Process this batch by serializing and writing to disk
                total_messages += len(batch_messages)
                log.info(f"Fetched {len(batch_messages)} messages in this batch ({total_messages} total)")

                for msg in batch_messages:
                    json.dump({"type": "message", "data": msg.to_dict(serialize_datetimes_to_str=True)}, f)
                    f.write("\n")

                # Fetch the next batch
                last_message = batch_messages[-1]
                batch_messages = engagement_db.get_messages(
                    firestore_query_filter=lambda q: q.order_by("message_id").start_after(last_message.to_dict()).limit(BATCH_SIZE)
                )
            log.info(f"Exported {total_messages} messages")

            log.info(f"Exporting history...")
            total_history_entries = 0
            batch_history_entries = engagement_db.get_history(firestore_query_filter=lambda q: q.order_by("history_entry_id").limit(BATCH_SIZE))
            while len(batch_history_entries) > 0:
                # Process this batch by serializing and writing to disk
                total_history_entries += len(batch_history_entries)
                log.info(f"Fetched {len(batch_history_entries)} history entries in this batch ({total_history_entries} total)")

                for entry in batch_history_entries:
                    json.dump({"type": "history_entry", "data": entry.to_dict(serialize_datetimes_to_str=True)}, f)
                    f.write("\n")

                # Fetch the next batch
                last_entry = batch_history_entries[-1]
                batch_history_entries = engagement_db.get_history(
                    firestore_query_filter=lambda q: q.order_by("history_entry_id").start_after(last_entry.to_dict()).limit(BATCH_SIZE)
                )
            log.info(f"Exported {total_history_entries} history entries")

        log.info(f"Compressing the exported data to a temporary file at '{raw_export_path}'...")
        with open(raw_export_path, "rb") as raw_file, gzip.open(compressed_export_path, "wb") as compressed_file:
            compressed_file.writelines(raw_file)

        if gzip_export_file_path is not None:
            log.warning(f"Copying the export to local disk at '{gzip_export_file_path}'...")
            with open(gzip_export_file_path, "wb") as f:
                shutil.copyfile(compressed_export_path, gzip_export_file_path)

        if gcs_upload_path is not None:
            log.info(f"Uploading the export to {gcs_upload_path}...")
            with open(compressed_export_path) as f:
                google_cloud_utils.upload_file_to_blob(google_cloud_credentials_file_path, gcs_upload_path, f)
