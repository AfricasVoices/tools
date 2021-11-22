import argparse
import json
from collections import defaultdict

from core_data_modules.logging import Logger
from engagement_database import EngagementDatabase
from engagement_database.data_models import HistoryEntry
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

BATCH_SIZE = 500

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Restores an engagement database from a jsonl file")

    parser.add_argument("--dry-run", const=True, default=False, action="store_const")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("restore_jsonl_file_path",
                        help="Path to a jsonl file to restore, in the format generated by export_engagement_database.py")
    parser.add_argument("engagement_database_credentials_file_url", metavar="engagement-database-credentials-file-url",
                        help="GS URL of the credentials for the Firestore project to export")
    parser.add_argument("database_path", metavar="database-path",
                        help="Path to the engagement database to restore to e.g. engagement_databases/test")

    args = parser.parse_args()

    dry_run = args.dry_run
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    restore_jsonl_file_path = args.restore_jsonl_file_path
    engagement_database_credentials_file_url = args.engagement_database_credentials_file_url
    database_path = args.database_path

    dry_run_text = ' (dry run)' if dry_run else ''
    log.info(f"Running an engagement database restore to {database_path}{dry_run_text}")

    log.info("Downloading engagement db credentials...")
    engagement_database_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        engagement_database_credentials_file_url
    ))

    engagement_db = EngagementDatabase.init_from_credentials(engagement_database_credentials, database_path)

    history_entries = []
    other_docs_count = 0
    log.info(f"Loading data to restore from '{restore_jsonl_file_path}'...")
    with open(restore_jsonl_file_path) as f:
        for line in f:
            d = json.loads(line)
            if d["type"] == HistoryEntry.DOC_TYPE:
                history_entries.append(HistoryEntry.from_dict(d["data"]))
            else:
                # Don't load other docs as we'll restore this directly from the latest history entries
                other_docs_count += 1
    log.info(f"Loaded {len(history_entries)} history entries (skipped {other_docs_count} other doc types)")

    # Restore the history entries
    log.info(f"Restoring history entries...")
    batch = engagement_db.batch()
    batch_size = 0
    restored = 0
    for history_entry in history_entries:
        engagement_db.restore_history_entry(history_entry, transaction=batch)
        batch_size += 1
        if batch_size >= BATCH_SIZE:
            if not dry_run:
                batch.commit()
            restored += batch_size
            log.info(f"Restored {restored}/{len(history_entries)} history entries")
            batch = engagement_db.batch()
            batch_size = 0
    if batch_size > 0:
        if not dry_run:
            batch.commit()
        restored += batch_size
        log.info(f"Restored {restored}/{len(history_entries)} history entries")

    log.info(f"Restoring documents in history...")
    # Find the latest history entry for each db_update_path
    latest_history_entries = dict()  # of db_update_path -> history entry
    for history_entry in history_entries:
        doc_path = history_entry.db_update_path
        if doc_path not in latest_history_entries:
            latest_history_entries[doc_path] = history_entry
        if history_entry.timestamp > latest_history_entries[doc_path].timestamp:
            latest_history_entries[doc_path] = history_entry

    # Restore the docs in the latest history entries
    batch = engagement_db.batch()
    batch_size = 0
    restored = 0
    restored_by_doc_type = defaultdict(int)  # of doc_type -> count
    for history_entry in latest_history_entries.values():
        engagement_db.restore_doc(history_entry.updated_doc, history_entry.db_update_path, transaction=batch)
        restored_by_doc_type[history_entry.doc_type] += 1
        batch_size += 1
        if batch_size >= BATCH_SIZE:
            if not dry_run:
                batch.commit()
            restored += batch_size
            log.info(f"Restored {restored}/{len(latest_history_entries)} documents from history")
            batch = engagement_db.batch()
            batch_size = 0
    if batch_size > 0:
        if not dry_run:
            batch.commit()
        restored += batch_size
        log.info(f"Restored {restored}/{len(latest_history_entries)} documents from history")

    log.info("")
    log.info(f"Summary of actions{dry_run_text}:")
    log.info(f"Restored {len(history_entries)} history entries")
    for doc_type, count in restored_by_doc_type.items():
        log.info(f"Restored {count} {doc_type} documents")