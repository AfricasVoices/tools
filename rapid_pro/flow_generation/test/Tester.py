import time

from core_data_modules.logging import Logger

log = Logger(__name__)


class Tester:
    """
    Provides a convenient, high-level API for testing flows.

    :param test_service: The service that hosts the flows to be tested.
    :type test_service: flow_generation.test.RapidProTestService.RapidProTestService
    :param test_participant_conversation: A conversation between a test participant and the `test_service`.
    :type test_participant_conversation: flow_generation.test.TelegramParticipantConversation.TelegramParticipantConversation
    :param reply_wait_time_seconds: Time to sleep for at the start of methods which check for replies from the test
                                    service to the participant.
                                    This wait period gives the workspace being tested time to respond to previously
                                    sent messages.
    :type reply_wait_time_seconds: int
    """
    def __init__(self, test_service, test_participant_conversation, reply_wait_time_seconds=3):
        self._service = test_service
        self._participant = test_participant_conversation

        self._last_message_id = self._participant.get_last_message_id()
        self._responses_wait_time_seconds = reply_wait_time_seconds

    def reset_participant(self):
        """
        Completely resets the test participant on the test service, so they are treated like a new participant on the
        next interaction.
        """
        log.info("Resetting participant...")
        self._service.reset_participant(self._participant.urn())

    def trigger_flow(self, flow_name):
        """
        Triggers a flow to the test participant.

        :param flow_name: Name of the flow to trigger.
        :type flow_name: str
        """
        log.info(f"Triggering flow '{flow_name}'...")
        self._service.trigger_flow(flow_name, self._participant.urn())

    def send_message(self, text):
        """
        Sends a message from the participant to the service under test.

        :param text: Text to send.
        :type text: str
        """
        log.info(f"Sending message '{text}'...")
        self._last_message_id = self._participant.send_message(text)

    def expect_replies(self, expected_replies):
        """
        Checks that the replies received from the service under test since the last action match the given texts.

        :param expected_replies: Texts of the expected replies.
        :type expected_replies: list of str
        """
        log.debug(f"Waiting {self._responses_wait_time_seconds} seconds for replies from Rapid Pro...")
        time.sleep(self._responses_wait_time_seconds)

        log.info(f"Checking replies match the expected replies {expected_replies}...")
        replies = self._participant.get_messages_after(self._last_message_id)
        assert replies == expected_replies, f"Expected replies {expected_replies} but received {replies}"

    def expect_latest_result(self, flow_name, result_name, expected_result):
        """
        Checks that the latest result for the participant in the test service matches the given text.

        :param flow_name: Flow to check for results.
        :type flow_name: str
        :param result_name: Name of result to check.
        :type result_name: str
        :param expected_result: Expected result.
        :type: str
        """
        log.debug(f"Checking Rapid Pro result '{result_name}' matches the expected result '{expected_result}'...")
        result = self._service.get_latest_result(flow_name, result_name, self._participant.urn())
        assert result == expected_result, \
            f"Expected '{result_name}' to have result '{expected_result}' but received '{result}'"
