import abc
import uuid


def generate_rapid_pro_uuid():
    return str(uuid.uuid4())


class DefinitionFile:
    def __init__(self, flow_graphs):
        self._flow_graphs = flow_graphs

    def to_rapid_pro_dict(self):
        return {
            "version": "13",
            "flows": [flow_graph.to_rapid_pro_dict() for flow_graph in self._flow_graphs],
            "campaigns": [],
            "triggers": [],
            "fields": [],
            "groups": []
        }


class FlowGraph:
    def __init__(self, name, primary_language, start_node):
        self._uuid = generate_rapid_pro_uuid()
        self._name = name
        self._primary_language = primary_language
        self._nodes = [start_node]

    def to_rapid_pro_dict(self):
        return {
            "uuid": self._uuid,
            "name": self._name,
            "expire_after_minutes": 60 * 24 * 7,  # 1 week
            "language": self._primary_language,
            "localization": {},
            "spec_version": "13.2.0",
            "type": "messaging",
            "revision": 1,
            "nodes": [self._nodes[0].to_rapid_pro_dict()],
        }


class FlowNode(abc.ABC):
    def __init__(self):
        self._uuid = generate_rapid_pro_uuid()


class WaitForResponseNode(FlowNode):
    def __init__(self, result_name, opt_out_detectors):
        super().__init__()
        self._result_name = result_name
        self._opt_out_detectors = opt_out_detectors

    def to_rapid_pro_dict(self):
        opt_out_category_uuid = generate_rapid_pro_uuid()
        default_category_uuid = generate_rapid_pro_uuid()

        stop_exit_uuid = generate_rapid_pro_uuid()
        other_exit_uuid = generate_rapid_pro_uuid()

        return {
            "uuid": self._uuid,
            "router": {
                "type": "switch",
                "default_category_uuid": default_category_uuid,
                "operand": "@input.text",
                "result_name": self._result_name,
                "wait": {
                    "type": "msg"
                },
                "cases": [detector.to_rapid_pro_dict(opt_out_category_uuid) for detector in self._opt_out_detectors],
                "categories": [
                    {
                        "exit_uuid": stop_exit_uuid,
                        "name": "Stop",
                        "uuid": opt_out_category_uuid
                    },
                    {
                        "exit_uuid": other_exit_uuid,
                        "name": "Other",
                        "uuid": default_category_uuid
                    }
                ]
            },
            "exits": [
                {
                    "destination_uuid": None,
                    "uuid": stop_exit_uuid
                },
                {
                    "destination_uuid": None,
                    "uuid": other_exit_uuid
                }
            ],
            "actions": [],
        }


class OptOutDetector(abc.ABC):
    def __init__(self):
        self._uuid = generate_rapid_pro_uuid()

    @abc.abstractmethod
    def to_rapid_pro_dict(self, category_uuid):
        pass


class RegexOptOutDetector(OptOutDetector):
    def __init__(self, regex):
        super().__init__()
        self._regex = regex

    def to_rapid_pro_dict(self, category_uuid):
        return {
            "arguments": [self._regex],
            "type": "has_pattern",
            "uuid": generate_rapid_pro_uuid(),
            "category_uuid": category_uuid
        }


class ExactMatchOptOutDetector(OptOutDetector):
    def __init__(self, exact_phrase):
        super().__init__()
        self._exact_phrase = exact_phrase

    def to_rapid_pro_dict(self, category_uuid):
        return {
            "arguments": [self._exact_phrase],
            "type": "has_only_phrase",
            "uuid": generate_rapid_pro_uuid(),
            "category_uuid": category_uuid
        }
