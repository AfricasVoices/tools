import argparse

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from rapid_pro_tools.rapid_pro_client import RapidProClient

log = Logger(__name__)
log.set_project_name("SynchroniseContacts")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrates contacts from one Rapid Pro instance to another")

    parser.add_argument("-f", "--force", const=True, default=False, action="store_const",
                        help="Overwrite contacts which differ in each instance with the latest")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("instance_1_domain", metavar="instance-1-domain",
                        help="Domain that the first instance of Rapid Pro is running on")
    parser.add_argument("instance_1_credentials_url", metavar="instance-1-credentials-url",
                        help="GS URL to the organisation access token file for authenticating to the first instance")
    parser.add_argument("instance_2_domain", metavar="instance-2-domain",
                        help="Domain that the second instance of Rapid Pro is running on")
    parser.add_argument("instance_2_credentials_url", metavar="instance-2-credentials-url",
                        help="GS URL to the organisation access token file for authenticating to the second instance")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    force_update = args.force

    instance_1_domain = args.instance_1_domain
    instance_1_credentials_url = args.instance_1_credentials_url
    instance_2_domain = args.instance_2_domain
    instance_2_credentials_url = args.instance_2_credentials_url

    # Initialise the two instances
    log.info("Downloading the access token for instance 1...")
    instance_1_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, instance_1_credentials_url).strip()
    instance_1 = RapidProClient(instance_1_domain, instance_1_token)

    log.info("Downloading the access token for instance 2...")
    instance_2_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, instance_2_credentials_url).strip()
    instance_2 = RapidProClient(instance_2_domain, instance_2_token)

    # Synchronise the contact fields
    log.info("Synchronising contact fields...")
    instance_1_fields = instance_1.get_fields()
    instance_2_fields = instance_2.get_fields()
    for field in instance_1_fields:
        if field.key not in {f.key for f in instance_2_fields}:
            instance_2.create_field(field.label)
    for field in instance_2_fields:
        if field.key not in {f.key for f in instance_1_fields}:
            instance_1.create_field(field.label)
    log.info("Contact fields synchronised")

    # Synchronise the contacts
    log.info("Synchronising contacts...")
    instance_1_contacts = instance_1.get_raw_contacts()
    instance_2_contacts = instance_2.get_raw_contacts()
    
    def filter_valid_contacts(contacts):
        valid_contacts = []
        for contact in contacts:
            if len(contact.urns) != 1:
                log.warning(f"Found a contact with multiple URNS; skipping. "
                            f"The RapidPro UUID is '{contact.uuid}'")
                continue
            if contact.urns[0].startswith("tel:") and not contact.urns[0].startswith("tel:+"):
                log.warning(f"Found a contact with a telephone number but without a country code; skipping. "
                            f"The RapidPro UUID is '{contact.uuid}'")
                continue
            valid_contacts.append(contact)
        return valid_contacts

    log.info("Filtering out invalid contacts from instance 1...")
    instance_1_contacts = filter_valid_contacts(instance_1_contacts)
    log.info("Filtering out invalid contacts from instance 2...")
    instance_2_contacts = filter_valid_contacts(instance_2_contacts)

    # Trim URN metadata because although Rapid Pro sometimes provides some in its get APIs, it refuses them when setting
    instance_1_contacts_lut = {c.urns[0].split("#")[0]: c for c in instance_1_contacts}
    instance_2_contacts_lut = {c.urns[0].split("#")[0]: c for c in instance_2_contacts}
    
    # Set names that are empty string to None, because while RapidPro can return empty string contact names,
    # it doesn't accept them when uploading
    for contact in instance_1_contacts:
        if contact.name == "":
            contact.name = None
    for contact in instance_2_contacts:
        if contact.name == "":
            contact.name = None

    # Update contacts present in instance 1 but not in instance 2
    urns_unique_to_instance_1 = instance_1_contacts_lut.keys() - instance_2_contacts_lut.keys()
    for i, urn in enumerate(urns_unique_to_instance_1):
        contact = instance_1_contacts_lut[urn]
        log.info(f"Adding new contacts to instance 2: {i + 1}/{len(urns_unique_to_instance_1)} "
                 f"(Rapid Pro UUID '{contact.uuid}' in instance 1)")
        instance_2.update_contact(contact.urns[0], contact.name, contact.fields)

    # Update contacts present in instance 2 but not in instance 1
    urns_unique_to_instance_2 = instance_2_contacts_lut.keys() - instance_1_contacts_lut.keys()
    for i, urn in enumerate(urns_unique_to_instance_2):
        contact = instance_2_contacts_lut[urn]
        log.info(f"Adding new contacts to instance 1: {i + 1}/{len(urns_unique_to_instance_2)} "
                 f"(Rapid Pro UUID '{contact.uuid}' in instance 2)")
        instance_1.update_contact(contact.urns[0], contact.name, contact.fields)

    # Update contacts present in both instances
    urns_in_both_instances = instance_1_contacts_lut.keys() & instance_2_contacts_lut.keys()
    for i, urn in enumerate(sorted(urns_in_both_instances)):
        contact_v1 = instance_1_contacts_lut[urn]
        contact_v2 = instance_2_contacts_lut[urn]

        if contact_v1.name == contact_v2.name and contact_v1.fields == contact_v2.fields:
            log.info(f"Synchronising contacts in both instances {i + 1}/{len(urns_in_both_instances)}: "
                     f"Contacts identical."
                     f"Rapid Pro UUIDs are '{contact_v1.uuid}' in instance 1; '{contact_v2.uuid}' in instance 2")
            continue

        # Contacts differ
        if not force_update:
            log.warning(f"Synchronising contacts in both instances {i + 1}/{len(urns_in_both_instances)}: "
                        f"Contacts differ, but not overwriting. Use --force to write the latest everywhere. "
                        f"Rapid Pro UUIDs are '{contact_v1.uuid}' in instance 1; '{contact_v2.uuid}' in instance 2")
            continue
            
        # Assume the most recent contact is correct
        # IMPORTANT: If the same contact has been changed on both Rapid Pro instances since the last sync was performed,
        #            the older changes will be overwritten.
        if contact_v1.modified_on > contact_v2.modified_on:
            log.info(f"Synchronising contacts in both instances {i + 1}/{len(urns_in_both_instances)}: "
                     f"Contacts differ, overwriting the contact in instance 2 with the more recent one in instance 1. "
                     f"Rapid Pro UUIDs are '{contact_v1.uuid}' in instance 1; '{contact_v2.uuid}' in instance 2")
            instance_2.update_contact(urn, contact_v1.name, contact_v1.fields)
        else:
            log.info(f"Synchronising contacts in both instances {i + 1}/{len(urns_in_both_instances)}: "
                     f"Contacts differ, overwriting the contact in instance 1 with the more recent one in instance 2. "
                     f"Rapid Pro UUIDs are '{contact_v1.uuid}' in instance 1; '{contact_v2.uuid}' in instance 2")
            instance_1.update_contact(urn, contact_v2.name, contact_v2.fields)
