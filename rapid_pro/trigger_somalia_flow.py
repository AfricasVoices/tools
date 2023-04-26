import argparse
import json
from datetime import datetime, time
from time import sleep

import pytz
from core_data_modules.logging import Logger
from dateutil.parser import isoparse
from storage.google_cloud import google_cloud_utils
from rapid_pro_tools.rapid_pro_client import RapidProClient

log = Logger(__name__)


def get_allowed_time_ranges():
    # TODO: Allow these to be specified externally
    allowed_trigger_time_ranges = [
        # Avoid nights (8pm - 7am EAT) and prayer times.
        # Approximate prayer times (EAT) are:
        #  - 11:40am - 12:40pm
        #  - 3pm - 3:50pm
        #  - 5:40pm - 6pm
        #  - 6:50pm - 7:30pm
        # Using a 10-minute buffer around prayer times, this gives the allowed ranges:
        ("07:00", "11:30"),
        ("12:50", "14:50"),
        ("16:00", "17:30"),
        ("18:10", "18:40"),
        ("19:40", "20:00")
    ]

    # Convert the allowed times to tz-aware date-time objects in Somalia time.
    allowed_trigger_time_ranges = [
        (time.fromisoformat(time_range[0] + ":00+03:00"), time.fromisoformat(time_range[1] + ":00+03:00"))
        for time_range in allowed_trigger_time_ranges
    ]

    return allowed_trigger_time_ranges


def time_allowed(time_to_check, allowed_time_ranges):
    for (start, end) in allowed_time_ranges:
        if start < time_to_check <= end:
            return True

    return False


def get_current_time():
    # pytz.utc.localize(datetime.utcnow()).time() isn't tz-aware, so create a tz-aware time via isoformat instead.
    return time.fromisoformat(pytz.utc.localize(datetime.utcnow()).time().isoformat() + "+00:00")


def load_urns(json_file_path):
    with open(json_file_path) as f:
        return json.load(f)


def start_rapid_pro_flow_for_urn(rapid_pro, flow_id, urn):
    log.warning(f"Starting flow for URN {urn}...")
    # TODO: Add a start_flow method to RapidProClient so we don't need to call the api directly via rapid_pro.rapid_pro
    rapid_pro.rapid_pro.create_flow_start(flow=flow_id, urns=[urn])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Triggers a flow to the specified URNs, one at a time, while "
                                                 "automatically excluding bad time ranges for Somalia")

    parser.add_argument("--dry-run", action="store_true",
                        help="Logs what would be triggered without actually triggering anything. This flag only "
                             "applies to the triggers themselves, and does not affect sleep calls")
    parser.add_argument("--start-date-time",
                        help="Datetime to start triggering the advert after, in ISO 8601 datetime format")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("rapid_pro_domain", help="URL of the Rapid Pro server to download data from")
    parser.add_argument("rapid_pro_token_file_url", metavar="rapid-pro-token-file-url",
                        help="GS URLs of a text file containing the authorisation token for the Rapid Pro server")
    parser.add_argument("flow_name", metavar="flow-name",
                        help="Name of the flow to trigger in rapid pro")
    parser.add_argument("urn_json_file_path", metavar="urn-json-file-path",
                        help="Local path to a json file containing the URNs to trigger the flow to")
    parser.add_argument("trigger_interval_seconds", metavar="trigger-interval-seconds", type=int,
                        help="Time to wait between each trigger, in seconds")

    args = parser.parse_args()

    dry_run = args.dry_run
    start_date_time = None if args.start_date_time is None else isoparse(args.start_date_time)
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    rapid_pro_domain = args.rapid_pro_domain
    rapid_pro_token_file_url = args.rapid_pro_token_file_url
    flow_name = args.flow_name
    urn_json_file_path = args.urn_json_file_path
    trigger_interval_seconds = args.trigger_interval_seconds

    dry_run_text = " (dry run)" if dry_run else ""
    log.info(f"Triggering flow {flow_name}{dry_run_text}")

    # Initialise Rapid Pro client
    rapid_pro_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, rapid_pro_token_file_url).strip()
    rapid_pro = RapidProClient(rapid_pro_domain, rapid_pro_token)

    allowed_time_ranges = get_allowed_time_ranges()
    urns = load_urns(urn_json_file_path)
    flow_id = rapid_pro.get_flow_id(flow_name)

    # Wait until the start datetime, if specified.
    if start_date_time is not None:
        current_time = pytz.utc.localize(datetime.utcnow())
        while current_time < start_date_time:
            time_until_start = start_date_time - current_time
            log.info(f"Waiting until '{start_date_time}' to trigger the flow, in {time_until_start}. "
                     f"Checking again in 5 minutes...")
            sleep(60 * 5)
            current_time = pytz.utc.localize(datetime.utcnow())

    start_date = datetime.utcnow()

    for i, urn in enumerate(urns):
        log.info(f"Processing URN {i + 1}/{len(urns)}...")
        current_time = get_current_time()
        while not time_allowed(current_time, allowed_time_ranges):
            # TODO: Instead of sleeping for 5 mins at a time, compute the time until the next allowed time range
            #       and sleep until then.
            log.debug(f"Time {current_time} not in an allowed range, sleeping for 5 mins")
            sleep(60 * 5)
            current_time = get_current_time()

        if not dry_run:
            start_rapid_pro_flow_for_urn(rapid_pro, flow_id, urn)

        log.debug(f"Sleeping for {trigger_interval_seconds} seconds before triggering the next participant")
        sleep(trigger_interval_seconds)

    end_date = datetime.utcnow()
    run_time = end_date - start_date

    log.info(f"Done{dry_run_text}. Triggered to {len(urns)} urns. Took {run_time}.")
