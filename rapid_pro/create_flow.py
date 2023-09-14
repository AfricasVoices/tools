import json

from flow_generation.FlowGraph import FlowGraph, WaitForResponseNode, RegexOptOutDetector, DefinitionFile, \
    ExactMatchOptOutDetector

opt_out_detectors = [
    RegexOptOutDetector("^j[ao]+w*ji"),
    ExactMatchOptOutDetector("Stop")
]

flow = FlowGraph(
    name="generated_activation_test",
    primary_language="eng",
    start_node=WaitForResponseNode(
        result_name="test_rqa",
        opt_out_detectors=opt_out_detectors
    )
)

definitions = DefinitionFile(
    flow_graphs=[flow]
)

with open("test_defs.json", "w") as f:
    json.dump(definitions.to_rapid_pro_dict(), f, indent=2, sort_keys=True)
