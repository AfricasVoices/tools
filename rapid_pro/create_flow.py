import json

from flow_generation.FlowGraph import (FlowGraph, AskQuestionIfNotAnswered, RegexOptOutDetector,
                                       DefinitionFile, ExactMatchOptOutDetector, ContactField, NodeSequence,
                                       SetContactFieldNode, SendMessageNode)

opt_out_detectors = [
    RegexOptOutDetector("^j[ao]+w*ji"),
    ExactMatchOptOutDetector("Stop")
]

age_field = ContactField(
    key="generated_flow_age",
    name="Generated Flow Age"
)

consent_field = ContactField(
    key="generated_flow_consent_withdrawn",
    name="Generated Flow Consent Withdrawn"
)

opt_out_sequence = NodeSequence([
    SendMessageNode(text="You will not receive any further messages"),
    SetContactFieldNode(consent_field, "yes")
])

flow = FlowGraph(
    name="generated_activation_test",
    primary_language="eng",
    start_node=AskQuestionIfNotAnswered(
        text="How old are you?",
        result_name="age_result",
        contact_field=age_field,
        opt_out_detectors=opt_out_detectors,
        opt_out_exit=opt_out_sequence
    )
)

definitions = DefinitionFile(
    flow_graphs=[flow]
)

with open("test_defs.json", "w") as f:
    json.dump(definitions.to_rapid_pro_dict(), f, indent=2, sort_keys=True)
