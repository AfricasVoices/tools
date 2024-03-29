import argparse
import json

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exports a list of all objects in a service account's Drive account")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("google_drive_credentials_url", metavar="google-drive-credentials-url",
                        help="GS URL to the Drive service account credentials file to use")
    parser.add_argument("jsonl_output_file_path", metavar="jsonl-output-file-path",
                        help="JSONL file to write the list of objects to")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    google_drive_credentials_url = args.google_drive_credentials_url
    jsonl_output_file_path = args.jsonl_output_file_path

    log.info("Initialising Google Drive client...")
    credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        google_drive_credentials_url
    ))
    drive_client_wrapper.init_client_from_info(credentials_info)

    log.info("Fetching info on all objects owned by the service account...")
    objects = drive_client_wrapper.list_all_objects_in_drive()
    log.info(f"Fetched info on {len(objects)} objects")

    objects.sort(key=lambda f: int(f["quotaBytesUsed"]))

    log.info(f"Exporting object info to '{jsonl_output_file_path}'...")
    with open(jsonl_output_file_path, "w") as f:
        for file in objects:
            json.dump(file, f)
            f.write("\n")
    log.info("Done")
