import json

from flow_generation.FlowGraph import (FlowGraph, SendMessageNode, WaitForResponseNode, RegexOptOutDetector,
                                       DefinitionFile, ExactMatchOptOutDetector, ContactFieldHasTextSplitNode,
                                       ContactField, SetContactFieldNode)

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
    # Ask the age question if not already answered
    start_node=ContactFieldHasTextSplitNode(
        contact_field=age_field,
        default_exit=SendMessageNode(
            text="How old are you?",
            default_exit=WaitForResponseNode(
                result_name="age_result",
                opt_out_detectors=opt_out_detectors,
                default_exit=SetContactFieldNode(
                    contact_field=age_field,
                    value="@results.age_result.input"
                )
            )
        )
    )
)

definitions = DefinitionFile(
    flow_graphs=[flow]
)

with open("test_defs.json", "w") as f:
    json.dump(definitions.to_rapid_pro_dict(), f, indent=2, sort_keys=True)
