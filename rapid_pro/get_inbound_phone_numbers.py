import argparse

from core_data_modules.logging import Logger
from core_data_modules.util import TimeUtils
from rapid_pro_tools.rapid_pro_client import RapidProClient
from storage.google_cloud import google_cloud_utils
from temba_client.v2 import Message

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads all inbound messages from Rapid Pro instances and exports "
                                                 "the phone numbers we heard from")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("rapid_pro_domain", help="URL of the Rapid Pro server to download data from")
    parser.add_argument("rapid_pro_token_file_url", metavar="rapid-pro-token-file-url",
                        help="GS URLs of a text file containing the authorisation token for the Rapid Pro server")
    parser.add_argument("output_file_path", metavar="output-file-path",
                        help="Output text file to write the phone numbers to")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    rapid_pro_domain = args.rapid_pro_domain
    rapid_pro_token_file_url = args.rapid_pro_token_file_url
    output_file_path = args.output_file_path

    log.info("Downloading the Rapid Pro access token...")
    rapid_pro_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, rapid_pro_token_file_url).strip()

    rapid_pro = RapidProClient(rapid_pro_domain, rapid_pro_token)

    all_messages = rapid_pro.get_raw_messages()
    inbound_messages = [msg for msg in all_messages if msg.direction == "in"]

    inbound_phone_numbers = set()
    for msg in inbound_messages:
        if msg.urn.startswith("tel:"):
            phone_number = msg.urn.split(":")[1]
            inbound_phone_numbers.add(phone_number)
        else:
            log.warning(f"Skipped non-telephone URN type {msg.urn.split(':')[0]}")

    log.warning(f"Exporting {len(inbound_phone_numbers)} inbound phone numbers to {output_file_path}...")
    with open(output_file_path, "w") as f:
        for number in inbound_phone_numbers:
            f.write(number + "\n")
    log.info(f"Done. Wrote {len(inbound_phone_numbers)} inbound phone numbers to {output_file_path}")
