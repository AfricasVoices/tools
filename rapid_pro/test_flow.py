import argparse
import json
import uuid

from core_data_modules.logging import Logger
from rapid_pro_tools.rapid_pro_client import RapidProClient
from storage.google_cloud import google_cloud_utils
from telethon.sync import TelegramClient

from flow_generation.FlowConfigurations import FlowConfigurations
from flow_generation.test.RapidProTestService import RapidProTestService
from flow_generation.test.TelegramParticipantConversation import TelegramParticipantConversation
from flow_generation.test.Tester import Tester

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tests flows on Rapid Pro by automating a Telegram conversation")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("rapid_pro_domain", help="URL of the Rapid Pro server to download data from")
    parser.add_argument("rapid_pro_token_file_url", metavar="rapid-pro-token-file-url",
                        help="GS URL of a text file containing the authorisation token for the Rapid Pro server")
    parser.add_argument("telegram_api_id", metavar="telegram-api-id",
                        help="Telegram API id, obtained via https://core.telegram.org/api/obtaining_api_id")
    parser.add_argument("telegram_api_hash", metavar="telegram-api-hash",
                        help="Telegram API hash, obtained via https://core.telegram.org/api/obtaining_api_id")
    parser.add_argument("telegram_bot_name", metavar="telegram-bot-name",
                        help="Handle of telegram bot to connect to e.g. @example_bot")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    rapid_pro_domain = args.rapid_pro_domain
    rapid_pro_token_file_url = args.rapid_pro_token_file_url
    telegram_api_id = args.telegram_api_id
    telegram_api_hash = args.telegram_api_hash
    telegram_bot_name = args.telegram_bot_name

    # TODO: Move to command line argument.
    flow_configurations_file_path = "flow_generation/resources/test_flow_configurations.json"

    log.info("Downloading the Rapid Pro access token...")
    rapid_pro_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, rapid_pro_token_file_url).strip()
    rapid_pro_client = RapidProClient(rapid_pro_domain, rapid_pro_token)
    test_service = RapidProTestService(rapid_pro_client)
    log.info(f"Initialised Rapid Pro test service")

    log.info(f"Initialising the Telegram client...")
    with TelegramClient(f"telegram_test_session_{telegram_api_id}", telegram_api_id, telegram_api_hash) as client:
        test_participant = TelegramParticipantConversation(client, telegram_bot_name)
        log.info("Initialised Telegram client")

        tester = Tester(test_service, test_participant)

        with open(flow_configurations_file_path) as f:
            flow_configurations = FlowConfigurations.from_dict(json.load(f))

        # Run a very simple, hard-coded test for now
        flow = flow_configurations.flows[0]
        assert flow.flow_name == "generation_test_demographics"

        tester.reset_participant()
        tester.trigger_flow(flow.flow_name)
        tester.expect_replies([flow.questions[0].text.get_translation("eng")])
        test_msg = "test message:" + str(uuid.uuid4())  # Add a random uuid so we don't mistake this message for previous tests
        tester.send_message(test_msg)
        tester.expect_latest_result(flow.flow_name, flow.questions[0].result_name, test_msg)
