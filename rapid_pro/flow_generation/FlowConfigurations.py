import abc

from flow_generation.FlowGraph import ContactField, OutboundText


class Consent:
    """
    Configuration for consent-handling in flows.

    This configuration is used to:
     - Ensure opted-out participants do not receive messages.
     - Automatically detect opt-out messages, acknowledge an opt-out with a reply message,
       and record that participants have opted-out.

    :param opt_out_detection_languages: ISO-639-3 language codes for the languages to detect opt-outs in.
                                        The appropriate opt-out regexes will be applied automatically from standardised
                                        definitions for each language.
    :type opt_out_detection_languages: list of str
    :param opt_out_reply_text: Text to send back to participants when they are automatically detected as having
                               opted-out.
    :type opt_out_reply_text: OutboundText
    :param opted_out_contact_field: Contact field to use to check/set opt-out status.
    :type opted_out_contact_field: ContactField
    """
    def __init__(self, opt_out_detection_languages, opt_out_reply_text, opted_out_contact_field):
        self.opt_out_detection_languages = opt_out_detection_languages
        self.opt_out_reply_text = opt_out_reply_text
        self.opted_out_contact_field = opted_out_contact_field

    @classmethod
    def from_dict(cls, d):
        return cls(
            opt_out_detection_languages=d["OptOutDetectionLanguages"],
            opt_out_reply_text=OutboundText(
                translations=d["OptOutReplyText"],
            ),
            opted_out_contact_field=ContactField(
                key=d["OptedOutContactField"]["Key"],
                name=d["OptedOutContactField"]["Name"]
            )
        )


class Languages:
    """
    Configuration for languages to use in flows.

    :param editing_language: ISO-639-3 language code for the primary language e.g. "eng". This is the language
                             to use for editing flows, not the language to be used in messaging.
    :type editing_language: str
    :param localization_languages: ISO-639-3 language codes of the other languages that this flow should support.
    :type localization_languages: list of str
    """
    def __init__(self, editing_language, localization_languages):
        self.editing_language = editing_language
        self.localization_languages = localization_languages

    @classmethod
    def from_dict(cls, d):
        return cls(
            editing_language=d["EditingLanguage"],
            localization_languages=d["LocalizationLanguages"]
        )


class GlobalSettings:
    """
    Settings to apply to every flow in a group of flows.

    :param languages: Language configuration.
    :type languages: Languages
    :param consent: Configuration for consent-handling.
    :type consent: Consent
    """

    def __init__(self, languages, consent):
        self.languages = languages
        self.consent = consent

    @classmethod
    def from_dict(cls, d):
        return cls(
            languages=Languages.from_dict(d["Languages"]),
            consent=Consent.from_dict(d["Consent"])
        )


class SurveyFlowQuestion:
    """
    Configuration for a survey question to ask in a flow.

    :param text: Question text to send.
    :type text: flow_generation.FlowGraph.OutboundText
    :param contact_field: Contact field where the participant's answer should be stored.
                          If this field already contains a value, the question will not be asked.
    :type contact_field: ContactField
    :param result_name: Variable to save the participant's response under in Rapid Pro's flow results.
    :type result_name: str
    """

    def __init__(self, text, contact_field, result_name):
        self.text = text
        self.contact_field = contact_field
        self.result_name = result_name

    @classmethod
    def from_dict(cls, d):
        return cls(
            text=OutboundText(translations=d["Text"]),
            contact_field=ContactField(
                key=d["ContactField"]["Key"],
                name=d["ContactField"]["Name"]
            ),
            result_name=d["ResultName"]
        )


class FlowConfiguration(abc.ABC):
    """
    Configuration for a flow.

    :param flow_name: Name of the flow.
    :type flow_name: str
    """

    def __init__(self, flow_name):
        self.flow_name = flow_name


class SurveyFlowConfiguration(FlowConfiguration):
    """
    Configuration for a survey flow.

    A survey flow asks the given questions in sequence. If any of the questions already have an answer, they are
    skipped.

    :param flow_name: Name of the flow.
    :type flow_name: str
    :param questions: Configuration for each question to be asked.
    :type questions: list of SurveyFlowQuestion
    """

    def __init__(self, flow_name, questions):
        super().__init__(flow_name)
        self.questions = questions

    @classmethod
    def from_dict(cls, d):
        return cls(
            flow_name=d["FlowName"],
            questions=[SurveyFlowQuestion.from_dict(q) for q in d["Params"]["Questions"]]
        )


class ActivationFlowConfiguration(FlowConfiguration):
    """
    Configuration for an activation flow.

    An activation flow waits for a response from the participant.
    On receiving a reply, enters another flow if specified.

    :param flow_name: Name of the flow.
    :type flow_name: str
    :param result_name: Variable to save the participant's response under in Rapid Pro's flow results.
    :type result_name: str
    :param next_flow: Name of the flow to enter after receiving a consenting response.
                      If None, does not enter another flow.
    :type next_flow: str | None
    """

    def __init__(self, flow_name, result_name, next_flow=None):
        super().__init__(flow_name)
        self.result_name = result_name
        self.next_flow = next_flow

    @classmethod
    def from_dict(cls, d):
        return cls(
            flow_name=d["FlowName"],
            result_name=d["Params"]["ResultName"],
            next_flow=d["Params"].get("NextFlow")
        )


class FlowConfigurations:
    """
    Configuration for flow generation and testing.

    :param global_settings: Global configuration which should apply to every flow defined in `flows`.
    :type global_settings: GlobalSettings
    :param flows: Configuration for each flow to generate/test.
    :type flows: list of FlowConfiguration
    """
    def __init__(self, global_settings, flows):
        self.global_settings = global_settings
        self.flows = flows

    @classmethod
    def from_dict(cls, d):
        return cls(
            global_settings=GlobalSettings.from_dict(d["GlobalSettings"]),
            flows=[FlowConfigurations._dict_to_flow_configuration(f) for f in d["Flows"]]
        )

    @staticmethod
    def _dict_to_flow_configuration(d):
        flow_type = d["FlowType"]
        if flow_type == "survey":
            return SurveyFlowConfiguration.from_dict(d)
        elif flow_type == "activation":
            return ActivationFlowConfiguration.from_dict(d)

        raise ValueError(f"Unknown FlowType '{flow_type}'")
