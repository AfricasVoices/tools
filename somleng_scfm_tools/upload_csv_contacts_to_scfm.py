import requests
import csv
import argparse

from core_data_modules.logging import Logger

log = Logger(__name__)
log.set_project_name("UploadCsvContactsToScfm")


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

        # Format contacts data to match scfm format
        contacts_to_upload = {}
        for contact in contacts_data:
            key = contact['Mobile Number']

            contacts_to_upload[key] = {'msisdn': contact['Mobile Number'],
                                       'metadata': {'location': contact['Location'], 'group': contact['Group']},}

    # Upload contacts to scfm
    api_endpoint = f'{scfm_instance_url}/api/contacts'

    uploaded_contacts = 0
    failed_contacts = 0
    failed_status_codes = []
    for contact in contacts_to_upload.values():
        print(contact)
        responce = requests.post(api_endpoint, auth=(scfm_contacts_api_key, ''),
                           data=contact)
        if responce.status_code == 201:
            uploaded_contacts +=1
        else:
            failed_contacts +=1
            failed_status_codes.append(responce.status_code)

    log.info(f'Uploaded {uploaded_contacts} contact(s) successfully')
    log.debug(f'{failed_contacts} contact(s) upload failed due to {failed_status_codes} reasons')
