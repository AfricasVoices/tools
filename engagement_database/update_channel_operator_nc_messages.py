import argparse
import json
import subprocess

from core_data_modules.cleaners import Codes, URNCleaner
from core_data_modules.logging import Logger
from engagement_database import EngagementDatabase
from engagement_database.data_models import CommandLogEntry, CommandStatuses, HistoryEntryOrigin
from google.cloud import firestore
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

BATCH_SIZE = 500


@firestore.transactional
def update_next_message_with_operator_nc(transaction, engagement_db, uuid_table, previous_message=None):
    if previous_message is None:
        query_filter = lambda q: q.order_by("last_updated").order_by("message_id") \
            .where("channel_operator", "==", Codes.NOT_CODED) \
            .limit(1)
    else:
        query_filter = lambda q: q.order_by("last_updated").order_by("message_id") \
            .start_after(previous_message.to_dict()) \
            .where("channel_operator", "==", Codes.NOT_CODED) \
            .limit(1)

    messages = engagement_db.get_messages(query_filter, transaction)
    if len(messages) == 0:
        return None
    message = messages[0]

    urn = uuid_table.uuid_to_data(message.participant_uuid)
    operator = URNCleaner.clean_operator(urn)

    if operator == Codes.NOT_CODED:
        log.warning(f"Message {message.message_id} still has operator {Codes.NOT_CODED}")
        return message

    log.info(f"Updating message {message.message_id} to have operator {operator}{dry_run_text}...")
    message.channel_operator = operator

    if not dry_run:
        engagement_db.set_message(
            message,
            HistoryEntryOrigin("Update operator", {}),
            transaction
        )

    return message


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Updates the channel_operator of all messages in an engagement "
                                                 "database that are labelled with channel_operator 'NC', by "
                                                 "redetermining the channel operator from the message urn")

    parser.add_argument("--dry-run", const=True, default=False, action="store_const")
    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("engagement_database_credentials_file_url", metavar="engagement-database-credentials-file-url",
                        help="GS URL of the credentials for the Firestore project to export")
    parser.add_argument("database_path", metavar="database-path",
                        help="Path to the engagement database to export e.g. engagement_databases/test")
    parser.add_argument("uuid_table_credentials_url", metavar="uuid-table-credentials-url",
                        help="GS URL to the Firebase credentials file to use for the uuid table")
    parser.add_argument("uuid_table_name", metavar="uuid-table-name",
                        help="Name of the uuid table to use to re-identify a participant")
    parser.add_argument("uuid_prefix", metavar="uuid-prefix",
                        help="UUID prefix for the uuid_table")

    args = parser.parse_args()

    dry_run = args.dry_run
    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    engagement_database_credentials_file_url = args.engagement_database_credentials_file_url
    database_path = args.database_path
    uuid_table_credentials_url = args.uuid_table_credentials_url
    uuid_table_name = args.uuid_table_name
    uuid_prefix = args.uuid_prefix

    dry_run_text = " (dry run)" if dry_run else ""

    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
    HistoryEntryOrigin.set_defaults(user, project, "NA", commit)

    log.info("Downloading Firestore engagement database credentials...")
    engagement_database_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        engagement_database_credentials_file_url
    ))
    engagement_db = EngagementDatabase.init_from_credentials(engagement_database_credentials, database_path)
    log.info(f"Initialised the Engagement Database client")

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        uuid_table_credentials_url
    ))
    uuid_table = FirestoreUuidTable.init_from_credentials(
        firestore_uuid_table_credentials,
        uuid_table_name,
        uuid_prefix
    )
    log.info("Initialised the Firestore UUID table")

    if not dry_run:
        engagement_db.set_command_log_entry(CommandLogEntry(status=CommandStatuses.STARTED))

    log.info(f"Updating messages labelled with channel_operator {Codes.NOT_CODED}{dry_run_text}...")
    total_messages = 0
    msg = update_next_message_with_operator_nc(engagement_db.transaction(), engagement_db, uuid_table)
    while msg is not None:
        total_messages += 1
        log.info(f"Processed {total_messages} messages so far")
        msg = update_next_message_with_operator_nc(engagement_db.transaction(), engagement_db, uuid_table, msg)

    if not dry_run:
        engagement_db.set_command_log_entry(CommandLogEntry(status=CommandStatuses.COMPLETED_SUCCESSFULLY))

    log.info(f"Done. Processed {total_messages} messages{dry_run_text}")
