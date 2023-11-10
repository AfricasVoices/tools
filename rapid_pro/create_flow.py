import json

from flow_generation.FlowConfigurations import FlowConfigurations, SurveyFlowConfiguration, ActivationFlowConfiguration
from flow_generation.FlowGraph import (FlowGraph, AskQuestionIfNotAnswered, RegexOptOutDetector,
                                       DefinitionFile, ExactMatchOptOutDetector, NodeSequence,
                                       SetContactFieldNode, SendMessageNode, WaitForResponseNode, EnterAnotherFlowNode,
                                       generate_rapid_pro_uuid, ContactFieldHasTextSplitNode)

opt_out_detectors_by_language = {
    "som": RegexOptOutDetector("^j[ao]+w*ji"),
    "eng": ExactMatchOptOutDetector("Stop")
}


def create_opt_out_detectors_from_config(config):
    """
    :type config: flow_generation.FlowConfigurations.Consent
    :rtype: list of flow_generation.FlowGraph.OptOutDetector
    """
    return [opt_out_detectors_by_language[lang] for lang in config.opt_out_detection_languages]


def create_opt_out_handler_from_config(config):
    """
    :type config: flow_generation.FlowConfigurations.Consent
    :rtype: flow_generation.FlowGraph.FlowNodeGroup
    """
    return NodeSequence([
        SendMessageNode(text=config.opt_out_reply_text),
        SetContactFieldNode(config.opted_out_contact_field, "yes")
    ])


def create_survey_question_from_config(config, opt_out_detectors, opt_out_handler):
    """
    :type config: flow_generation.FlowConfigurations.SurveyFlowQuestion
    :type opt_out_detectors: list of flow_generation.FlowGraph.OptOutDetector
    :type opt_out_handler: flow_generation.FlowGraph.FlowNode | flow_generation.FlowGraph.FlowNode | Nones
    :rtype: flow_generation.FlowGraph.AskQuestionIfNotAnswered
    """
    return AskQuestionIfNotAnswered(
        text=config.text,
        contact_field=config.contact_field,
        opt_out_detectors=opt_out_detectors,
        opt_out_exit=opt_out_handler,
        result_name=config.result_name,
    )


def create_survey_flow_from_config(flow_config, flow_ids, global_settings, opt_out_detectors, opt_out_handler):
    """
    :type flow_config: flow_generation.FlowConfigurations.SurveyFlowConfiguration
    :type flow_ids: dict of str -> str
    :type global_settings: flow_generation.FlowConfigurations.GlobalSettings
    :type opt_out_detectors: list of flow_generation.FlowGraph.OptOutDetector
    :type opt_out_handler: flow_generation.FlowGraph.FlowNode | flow_generation.FlowGraph.FlowNode | Nones
    :rtype: flow_generation.FlowGraph.FlowGraph
    """
    consent_check = ContactFieldHasTextSplitNode(
        contact_field=global_settings.consent.opted_out_contact_field
    )

    question_nodes = []
    for question in flow_config.questions:
        question_node = create_survey_question_from_config(question, opt_out_detectors, opt_out_handler)
        if len(question_nodes) > 0:
            question_nodes[-1].previously_answered_exit_node = question_node
            question_nodes[-1].newly_answered_exit_node = question_node
        question_nodes.append(question_node)

    consent_check.default_exit = NodeSequence(question_nodes)

    return FlowGraph(
        uuid=flow_ids[flow_config.flow_name],
        name=flow_config.flow_name,
        editing_language=global_settings.languages.editing_language,
        localization_languages=global_settings.languages.localization_languages,
        start_node=consent_check
    )


def create_activation_flow_from_config(flow_config, flow_ids, global_settings, opt_out_detectors, opt_out_handler):
    """
    :type flow_config: flow_generation.FlowConfigurations.ActivationFlowConfiguration
    :type flow_ids: dict of str -> str
    :type global_settings: flow_generation.FlowConfigurations.GlobalSettings
    :type opt_out_detectors: list of flow_generation.FlowGraph.OptOutDetector
    :type opt_out_handler: flow_generation.FlowGraph.FlowNode | flow_generation.FlowGraph.FlowNode | Nones
    :rtype: flow_generation.FlowGraph.FlowGraph
    """
    next_flow_node = None
    if flow_config.next_flow is not None:
        next_flow_node = EnterAnotherFlowNode(flow_name=flow_config.next_flow, flow_uuid=flow_ids[flow_config.next_flow])

    return FlowGraph(
        uuid=flow_ids[flow_config.flow_name],
        name=flow_config.flow_name,
        editing_language=global_settings.languages.editing_language,
        localization_languages=global_settings.languages.localization_languages,
        start_node=WaitForResponseNode(
            result_name=flow_config.result_name,
            opt_out_detectors=opt_out_detectors,
            opt_out_exit=opt_out_handler,
            default_exit=next_flow_node
        )
    )


def create_flow_from_config(flow_config, flow_ids, global_settings, opt_out_detectors, opt_out_handler):
    """
    :type flow_config: flow_generation.FlowConfigurations.FlowConfiguration
    :type flow_ids: dict of str -> str
    :type global_settings: flow_generation.FlowConfigurations.GlobalSettings
    :type opt_out_detectors: list of flow_generation.FlowGraph.OptOutDetector
    :type opt_out_handler: flow_generation.FlowGraph.FlowNode | flow_generation.FlowGraph.FlowNode | Nones
    :rtype: flow_generation.FlowGraph.FlowGraph
    """
    if isinstance(flow_config, SurveyFlowConfiguration):
        return create_survey_flow_from_config(flow_config, flow_ids, global_settings, opt_out_detectors, opt_out_handler)
    elif isinstance(flow_config, ActivationFlowConfiguration):
        return create_activation_flow_from_config(flow_config, flow_ids, global_settings, opt_out_detectors, opt_out_handler)

    raise TypeError("Unknown FlowConfiguration type")


if __name__ == "__main__":
    # TODO: Move to command line argument.
    flow_configurations_file_path = "flow_generation/resources/test_flow_configurations.json"

    with open(flow_configurations_file_path) as f:
        flow_configurations = FlowConfigurations.from_dict(json.load(f))

    global_settings = flow_configurations.global_settings

    opt_out_detectors = create_opt_out_detectors_from_config(global_settings.consent)
    opt_out_handler = create_opt_out_handler_from_config(global_settings.consent)

    flow_ids = {flow_config.flow_name: generate_rapid_pro_uuid() for flow_config in flow_configurations.flows}

    flows = [
        create_flow_from_config(flow_config, flow_ids, global_settings, opt_out_detectors, opt_out_handler)
        for flow_config in flow_configurations.flows
    ]

    definitions = DefinitionFile(flow_graphs=flows)

    with open("test_defs.json", "w") as f:
        json.dump(definitions.to_rapid_pro_dict(), f, indent=2, sort_keys=True)
