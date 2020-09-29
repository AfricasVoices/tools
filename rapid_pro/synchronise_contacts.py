import argparse

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from rapid_pro_tools.rapid_pro_client import RapidProClient

log = Logger(__name__)
log.set_project_name("SynchroniseContacts")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synchronises contacts between two Rapid Pro workspaces")

    parser.add_argument("-f", "--force", const=True, default=False, action="store_const",
                        help="Overwrite contacts which differ in each workspace with the latest")
    parser.add_argument("--dry-run", const=True, default=False, action="store_const",
                        help="Logs the updates that would be made without actually updating any data in either "
                             "workspace")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("workspace_1_domain", metavar="workspace-1-domain",
                        help="Domain that the first workspace of Rapid Pro is running on")
    parser.add_argument("workspace_1_credentials_url", metavar="workspace-1-credentials-url",
                        help="GS URL to the organisation access token file for authenticating to the first workspace")
    parser.add_argument("workspace_2_domain", metavar="workspace-2-domain",
                        help="Domain that the second workspace of Rapid Pro is running on")
    parser.add_argument("workspace_2_credentials_url", metavar="workspace-2-credentials-url",
                        help="GS URL to the organisation access token file for authenticating to the second workspace")

    args = parser.parse_args()

    force_update = args.force
    dry_run = args.dry_run

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    workspace_1_domain = args.workspace_1_domain
    workspace_1_credentials_url = args.workspace_1_credentials_url
    workspace_2_domain = args.workspace_2_domain
    workspace_2_credentials_url = args.workspace_2_credentials_url

    if dry_run:
        log.info("Performing a dry-run")

    # Initialise the two Rapid Pro clients
    log.info("Downloading the access token for workspace 1...")
    workspace_1_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, workspace_1_credentials_url).strip()
    workspace_1 = RapidProClient(workspace_1_domain, workspace_1_token)
    workspace_1_name = workspace_1.get_workspace_name()
    log.info(f"Done. workspace 1 is called {workspace_1_name}")

    log.info("Downloading the access token for workspace 2...")
    workspace_2_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, workspace_2_credentials_url).strip()
    workspace_2 = RapidProClient(workspace_2_domain, workspace_2_token)
    workspace_2_name = workspace_2.get_workspace_name()
    log.info(f"Done. workspace 2 is called {workspace_2_name}")

    # Download the data from Rapid Pro
    log.info("Downloading contact fields...")
    log.info(f"Downloading all fields from {workspace_1_name}...")
    workspace_1_fields = workspace_1.get_fields()
    log.info(f"Downloading all fields from {workspace_2_name}...")
    workspace_2_fields = workspace_2.get_fields()

    # Synchronise the contacts
    log.info("Downloading contacts...")
    log.info(f"Downloading all contacts from {workspace_1_name}...")
    workspace_1_contacts = workspace_1.get_raw_contacts()
    log.info(f"Downloading all contacts from {workspace_2_name}...")
    workspace_2_contacts = workspace_2.get_raw_contacts()

    # If in dry_run mode, dereference workspace_1 and workspace_2 as an added safety. This prevents accidental
    # writes to either workspace.
    if dry_run:
        workspace_1 = None
        workspace_2 = None

    # Synchronise the data
    # Synchronise the contact fields
    log.info(f"Synchronising fields from {workspace_1_name} to {workspace_2_name}...")
    for field in workspace_1_fields:
        if field.key not in {f.key for f in workspace_2_fields}:
            if dry_run:
                log.info(f"Would create field {field.label}")
                continue
            workspace_2.create_field(field.label)
    log.info(f"Synchronising fields from {workspace_2_name} to {workspace_1_name}...")
    for field in workspace_2_fields:
        if field.key not in {f.key for f in workspace_1_fields}:
            if dry_run:
                log.info(f"Would create field {field.label}")
                continue
            workspace_1.create_field(field.label)
    log.info("Contact fields synchronised")

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

    log.info(f"Filtering out invalid contacts from {workspace_1_name}...")
    workspace_1_contacts = filter_valid_contacts(workspace_1_contacts)
    log.info(f"Filtering out invalid contacts from {workspace_2_name}...")
    workspace_2_contacts = filter_valid_contacts(workspace_2_contacts)

    # Trim URN metadata because although Rapid Pro sometimes provides some in its get APIs, it refuses them when setting
    workspace_1_contacts_lut = {c.urns[0].split("#")[0]: c for c in workspace_1_contacts}
    workspace_2_contacts_lut = {c.urns[0].split("#")[0]: c for c in workspace_2_contacts}
    
    # Set names that are empty string to None, because while RapidPro can return empty string contact names,
    # it doesn't accept them when uploading
    for contact in workspace_1_contacts:
        if contact.name == "":
            contact.name = None
    for contact in workspace_2_contacts:
        if contact.name == "":
            contact.name = None

    # Update contacts present in workspace 1 but not in workspace 2
    urns_unique_to_workspace_1 = workspace_1_contacts_lut.keys() - workspace_2_contacts_lut.keys()
    for i, urn in enumerate(urns_unique_to_workspace_1):
        contact = workspace_1_contacts_lut[urn]
        log.info(f"Adding new contacts to {workspace_2_name}: {i + 1}/{len(urns_unique_to_workspace_1)} "
                 f"(Rapid Pro UUID '{contact.uuid}' in {workspace_1_name})")
        if dry_run:
            continue
        workspace_2.update_contact(contact.urns[0], contact.name, contact.fields)

    # Update contacts present in workspace 2 but not in workspace 1
    urns_unique_to_workspace_2 = workspace_2_contacts_lut.keys() - workspace_1_contacts_lut.keys()
    for i, urn in enumerate(urns_unique_to_workspace_2):
        contact = workspace_2_contacts_lut[urn]
        log.info(f"Adding new contacts to {workspace_1_name}: {i + 1}/{len(urns_unique_to_workspace_2)} "
                 f"(Rapid Pro UUID '{contact.uuid}' in {workspace_2_name})")
        if dry_run:
            continue
        workspace_1.update_contact(contact.urns[0], contact.name, contact.fields)

    # Update contacts present in both workspaces
    urns_in_both_workspaces = workspace_1_contacts_lut.keys() & workspace_2_contacts_lut.keys()
    for i, urn in enumerate(sorted(urns_in_both_workspaces)):
        contact_v1 = workspace_1_contacts_lut[urn]
        contact_v2 = workspace_2_contacts_lut[urn]

        if contact_v1.name == contact_v2.name and contact_v1.fields == contact_v2.fields:
            log.debug(f"Synchronising contacts in both workspaces {i + 1}/{len(urns_in_both_workspaces)}: "
                      f"Contacts identical. "
                      f"(Rapid Pro UUIDs are '{contact_v1.uuid}' in {workspace_1_name}; "
                      f"'{contact_v2.uuid}' in {workspace_2_name})")
            continue

        # Contacts differ
        if not force_update:
            log.warning(f"Synchronising contacts in both workspaces {i + 1}/{len(urns_in_both_workspaces)}: "
                        f"Contacts differ, but not overwriting. Use --force to write the latest everywhere. "
                        f"(Rapid Pro UUIDs are '{contact_v1.uuid}' in {workspace_1_name}; "
                        f"'{contact_v2.uuid}' in {workspace_2_name})")
            continue
            
        # Assume the most recent contact is correct
        # IMPORTANT: If the same contact has been changed on both Rapid Pro workspaces since the last sync was
        #            performed, the older changes will be overwritten.
        if contact_v1.modified_on > contact_v2.modified_on:
            log.info(f"Synchronising contacts in both workspaces {i + 1}/{len(urns_in_both_workspaces)}: "
                     f"Contacts differ, overwriting the contact in {workspace_2_name} with the more recent one in "
                     f"{workspace_1_name}. "
                     f"(Rapid Pro UUIDs are '{contact_v1.uuid}' in {workspace_1_name}; "
                     f"'{contact_v2.uuid}' in {workspace_2_name})")
            if dry_run:
                continue
            workspace_2.update_contact(urn, contact_v1.name, contact_v1.fields)
        else:
            log.info(f"Synchronising contacts in both workspaces {i + 1}/{len(urns_in_both_workspaces)}: "
                     f"Contacts differ, overwriting the contact in {workspace_1_name} with the more recent one in "
                     f"{workspace_2_name}. "
                     f"(Rapid Pro UUIDs are '{contact_v1.uuid}' in {workspace_1_name}; "
                     f"'{contact_v2.uuid}' in {workspace_2_name})")
            if dry_run:
                continue
            workspace_1.update_contact(urn, contact_v2.name, contact_v2.fields)
