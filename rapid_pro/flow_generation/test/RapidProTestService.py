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

    def _iter_flow_runs_for_participant(self, flow_name, participant_urn):
        contact = self._rapid_pro_client.get_contacts(urn=participant_urn)[0]
        for batch in self._rapid_pro_client.rapid_pro.get_runs(contact=contact.uuid).iterfetches(retry_on_rate_exceed=True):
            for run in batch:
                if run.flow.name == flow_name:
                    yield run

    def get_latest_result(self, flow_name, result_name, participant_urn):
        """
        Gets the latest requested result from the latest flow run.

        :param flow_name: Flow to get result from.
        :type flow_name: str
        :param result_name: Name of the result to get from the flow.
        :type result_name: str
        :param participant_urn: Participant to get the result for.
        :type participant_urn: str
        :return: Latest `result_name` in flow `flow_name` for participant `participant_urn`.
        :rtype: str
        """
        for run in self._iter_flow_runs_for_participant(flow_name, participant_urn):
            if result_name in run.values:
                return run.values[result_name].input

        raise KeyError(f"No run found with result_name '{result_name}'")
