import argparse
import json

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deletes an object from Google Drive")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("google_drive_credentials_url", metavar="google-drive-credentials-url",
                        help="GS URL to the Drive service account credentials file to use")
    parser.add_argument("object_id", metavar="object-id",
                        help="Id of object in Google Drive to delete")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    google_drive_credentials_url = args.google_drive_credentials_url
    object_id = args.object_id

    log.info("Initialising Google Drive client...")
    credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        google_drive_credentials_url
    ))
    drive_client_wrapper.init_client_from_info(credentials_info)

    drive_client_wrapper.delete_object(object_id)
