import json

from flow_generation.FlowGraph import (FlowGraph, AskQuestionIfNotAnswered, RegexOptOutDetector,
                                       DefinitionFile, ExactMatchOptOutDetector, ContactField)

opt_out_detectors = [
    RegexOptOutDetector("^j[ao]+w*ji"),
    ExactMatchOptOutDetector("Stop")
]

age_field = ContactField(
    key="generated_flow_age",
    name="Generated Flow Age"
)

flow = FlowGraph(
    name="generated_activation_test",
    primary_language="eng",
    start_node=AskQuestionIfNotAnswered(
        text="How old are you?",
        result_name="age_result",
        contact_field=age_field,
        opt_out_detectors=opt_out_detectors
    )
)

definitions = DefinitionFile(
    flow_graphs=[flow]
)

with open("test_defs.json", "w") as f:
    json.dump(definitions.to_rapid_pro_dict(), f, indent=2, sort_keys=True)
