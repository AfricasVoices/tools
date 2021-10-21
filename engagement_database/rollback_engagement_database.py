import argparse
import json
import subprocess
from collections import defaultdict

from core_data_modules.logging import Logger
from dateutil.parser import isoparse
from engagement_database import EngagementDatabase
from engagement_database.data_models import HistoryEntryOrigin, Message
from google.cloud import firestore
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

BATCH_SIZE = 500

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Performs a hard rollback of data in an engagement database using history "
                    "(all messages are returned to the last version set before the rollback timestamp, and newer "
                    "history entries are deleted). "
                    "NOTE: This is a hard rollback, so will require any tools connected to this database to have their "
                    "caches cleared"
    )

    parser.add_argument("--dry-run", const=True, default=False, action="store_const")
    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("engagement_database_credentials_file_url", metavar="engagement-database-credentials-file-url",
                        help="GS URL of the credentials for the Firestore project to export")
    parser.add_argument("database_path", metavar="database-path",
                        help="Path to the engagement database to export e.g. engagement_databases/test")
    parser.add_argument("rollback_timestamp_inclusive", metavar="rollback-timestamp-inclusive",
                        help="Timestamp to rollback to, inclusive, as an ISO8601 string")

    args = parser.parse_args()

    dry_run = args.dry_run
    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    engagement_database_credentials_file_url = args.engagement_database_credentials_file_url
    database_path = args.database_path
    rollback_timestamp_inclusive = isoparse(args.rollback_timestamp_inclusive)

    dry_run_text = ' (dry run)' if dry_run else ''
    log.info(f"Starting rollback of engagement database {database_path} to timestamp "
             f"{rollback_timestamp_inclusive}{dry_run_text}")

    log.info("Downloading Firestore UUID Table credentials...")
    engagement_database_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        engagement_database_credentials_file_url
    ))
    engagement_db = EngagementDatabase.init_from_credentials(engagement_database_credentials, database_path)

    log.info(f"Fetching history entries modified on or since {rollback_timestamp_inclusive} that need rollback...")
    history_entries_to_rollback = engagement_db.get_history(
        firestore_query_filter=lambda q: q.where("timestamp", ">=", rollback_timestamp_inclusive))
    log.info(f"Fetched {len(history_entries_to_rollback)} history entries to rollback")

    db_update_path_to_history_entries = defaultdict(list)
    for history_entry in history_entries_to_rollback:
        db_update_path_to_history_entries[history_entry.db_update_path].append(history_entry)
    db_update_paths = db_update_path_to_history_entries.keys()
    log.info(f"Found {len(db_update_paths)} unique doc paths to rollback")

    # Iterative over each of the documents that we found history for, deleting that document or reverting it to the
    # latest version written before the rollback_timestamp, as appropriate, and deleting all the newer history entries,
    # in a single batch.
    deleted_docs = 0
    reverted_docs = 0
    deleted_history_entries = 0
    for i, db_update_path in enumerate(db_update_paths):
        # Get the last history entry made before `rollback_timestamp`, if it exists
        last_valid_history_entries = engagement_db.get_history(
            firestore_query_filter=lambda q: q
                .where("db_update_path", "==", db_update_path)
                .where("timestamp", "<", rollback_timestamp_inclusive)
                .order_by("timestamp", firestore.Query.DESCENDING)
                .limit(1)
        )

        batch = engagement_db.batch()
        if len(last_valid_history_entries) < 1:
            # There was no history entry made before the rollback timestamp, meaning this doc was created after the
            # roll_back timestamp. Therefore, delete this doc.
            log.info(f"Rolling back document {i + 1}/{len(db_update_paths)}: Deleting doc {db_update_path}{dry_run_text}...")
            if not dry_run:
                engagement_db.delete_doc(db_update_path, transaction=batch)
            deleted_docs += 1
        else:
            # Hard-rollback the doc to the last version set before `rollback_timestamp`.
            log.info(f"Rolling back document {i + 1}/{len(db_update_paths)}: Reverting doc {db_update_path}{dry_run_text}...")
            last_valid_history_entry = last_valid_history_entries[0]
            reverted_docs += 1
            if not dry_run:
                engagement_db.restore_doc(
                    last_valid_history_entry.updated_doc, last_valid_history_entry.db_update_path, transaction=batch
                )

        # Delete all the history entries modified after the rollback timestamp
        for history_entry in db_update_path_to_history_entries[db_update_path]:
            if not dry_run:
                engagement_db.delete_doc(f"history/{history_entry.history_entry_id}", transaction=batch)
            deleted_history_entries += 1
        batch.commit()

    log.info(f"Done. Summary of actions{dry_run_text}:")
    log.info(f"Deleted {deleted_docs} docs (excluding history entries)")
    log.info(f"Reverted {reverted_docs} docs (excluding history entries)")
    log.info(f"Deleted {deleted_history_entries} history entries")
