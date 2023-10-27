class TelegramParticipantConversation:
    """
    A conversation between a Telegram user and a Telegram bot, from the perspective of the Telegram user.

    :param client: Telethon Telegram client of the user.
    :type client: telethon.sync.TelegramClient
    :param bot_username: Bot username to converse with when sending/receiving messages e.g. "@test_bot"
    :type bot_username: str
    """
    def __init__(self, client, bot_username):
        self._client = client
        self._bot = client.get_entity(bot_username)

    def urn(self):
        """
        :return: This participant's URN.
        :rtype: str
        """
        telegram_id = (self._client.get_me()).id
        return f"telegram:{telegram_id}"

    def send_message(self, text):
        """
        Send a message from this participant to the bot.

        :param text: Text of message to send.
        :type text: str
        :return: Id of sent message.
        :rtype: str
        """
        return self._client.send_message(self._bot, text).id

    def get_last_message_id(self):
        """
        :return: Id of the most recent message in this conversation.
        :rtype: str
        """
        return self._client.get_messages(self._bot, limit=1)[0].id

    def get_messages_after(self, message_id):
        """
        :param message_id: Gets the texts of all the messages exchanged in this conversation since the message with
                           the given `message_id`.
        :return: list of str
        """
        messages = []
        for msg in self._client.get_messages(self._bot, min_id=message_id):
            messages.append(msg.text)
        return messages
