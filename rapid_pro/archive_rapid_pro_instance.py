import argparse
import tarfile
import tempfile

from core_data_modules.logging import Logger
from rapid_pro_tools.rapid_pro_client import RapidProClient
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Archives all the data in a Rapid Pro instance available via the "
                                                 "API to local disk and/or google cloud storage")

    parser.add_argument("--gzip-export-file-path",
                        help="tar.gzip file to write the exported data to")
    parser.add_argument("--gcs-upload-path",
                        help="GS URL to upload the exported tar.gzip to")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("rapid_pro_domain", help="URL of the Rapid Pro server to download data from")
    parser.add_argument("rapid_pro_token_file_url", metavar="rapid-pro-token-file-url",
                        help="GS URL of a text file containing the authorisation token for the Rapid Pro server")

    args = parser.parse_args()

    gzip_export_file_path = args.gzip_export_file_path
    gcs_upload_path = args.gcs_upload_path
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    rapid_pro_domain = args.rapid_pro_domain
    rapid_pro_token_file_url = args.rapid_pro_token_file_url

    if gzip_export_file_path is None and gcs_upload_path is None:
        log.error(f"No output locations specified. Please provide at least one of --gzip-export-file-path or "
                  f"--gcs-upload-path")
        exit(1)

    log.info("Downloading the Rapid Pro access token...")
    rapid_pro_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, rapid_pro_token_file_url).strip()

    rapid_pro = RapidProClient(rapid_pro_domain, rapid_pro_token)

    with tempfile.TemporaryDirectory() as export_directory_path:
        log.info(f"Downloading all data from the Rapid Pro instance to temporary directory '{export_directory_path}'...")
        rapid_pro.export_all_data(export_directory_path)

        if gzip_export_file_path is None:
            gzip_export_file_path = f"{export_directory_path}/"

        log.info(f"Zipping the exported data directory '{export_directory_path}' to '{gzip_export_file_path}'...")
        with tarfile.open(gzip_export_file_path, "w:gz") as tar:
            tar.add(export_directory_path, arcname="export")

        if gcs_upload_path is not None:
            log.info(f"Uploading the zipped file to {gcs_upload_path}...")
            with open(gzip_export_file_path, "rb") as f:
                google_cloud_utils.upload_file_to_blob(google_cloud_credentials_file_path, gcs_upload_path, f)
            log.info("Done")
