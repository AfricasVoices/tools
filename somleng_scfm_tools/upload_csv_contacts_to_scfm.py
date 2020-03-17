import requests
import csv
import argparse

from core_data_modules.logging import Logger

log = Logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrates a phone number <-> uuid table from Core Data Modules "
                                                 "to Firestore")

    parser.add_argument("scfm_instance_url", metavar="scfm-instance-url",
                        help="Url to the server hosting the scfm instance")
    parser.add_argument("scfm_contacts_api_key", metavar="scfm-contacts-api-key",
                        help="Api key that authenticates scfm read and write contacts operations")
    parser.add_argument("csv_input_file_path", metavar="csv-input-file-path",
                        help="Path to a csv file to read  to upload from")


    args = parser.parse_args()

    scfm_instance_url = args.scfm_instance_url
    scfm_contacts_api_key = args.scfm_contacts_api_key
    csv_input_file_path = args.csv_input_file_path

    # Read CVS into a dictionary
    with open(csv_input_file_path, 'r') as f:
        contacts_data = csv.DictReader(f)

        # Format contacts data to match SCFM format
        contacts_to_upload = {}
        for contact in contacts_data:
            key = contact['Mobile Number']

            contacts_to_upload[key] = {'msisdn': contact['Mobile Number'],
                                       'metadata': {'location': contact['Location'], 'group': contact['Group']}}

    # Upload contacts to Somleng
    api_endpoint = f'{scfm_instance_url}/api/contacts'

    for contact in contacts_to_upload.values():
        ct = requests.post(api_endpoint, scfm_contacts_api_key, contact)
        print(api_endpoint)
        print(scfm_contacts_api_key)
        print(contact)
        print(ct.status_code)
