class RapidProTestService:
    """
    API for a Rapid Pro workspace which contains flows under test.

    :param rapid_pro_client: Rapid Pro client to use to connect to Rapid Pro.
    :type rapid_pro_client: rapid_pro_tools.rapid_pro_client.RapidProClient
    """
    def __init__(self, rapid_pro_client):
        self._rapid_pro_client = rapid_pro_client

    def reset_participant(self, urn):
        """
        Completely resets this participant in Rapid Pro, by clearing all their contact fields' values.

        :param urn: URN of the contact to reset.
        :type urn: str
        """
        contacts = self._rapid_pro_client.get_contacts(urn=urn)
        assert len(contacts) == 1, contacts
        contact = contacts[0]

        self._rapid_pro_client.update_contact(urn=urn, contact_fields={k: None for k in contact.fields.keys()})

    def trigger_flow(self, flow_name, urn):
        """
        Immediately triggers a flow to a contact.

        :param flow_name: Name of the flow to trigger.
        :type flow_name: str
        :param urn: URN of the contact to trigger the flow to.
        :type urn: str
        """
        flow_id = self._rapid_pro_client.get_flow_id(flow_name)
        self._rapid_pro_client.rapid_pro.create_flow_start(flow=flow_id, urns=[urn])
