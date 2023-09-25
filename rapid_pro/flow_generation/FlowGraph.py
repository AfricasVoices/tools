import abc
import uuid


def generate_rapid_pro_uuid():
    return str(uuid.uuid4())


class DefinitionFile:
    """
    Represents a Rapid Pro definition file, which can be exported and uploaded to Rapid Pro via its UI.

    :param flow_graphs: FlowGraphs in this definition file.
    :type flow_graphs: list of FlowGraph
    """

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
    """
    Represents a Rapid Pro flow.

    :param name: Name of this flow.
    :type name: str
    :param primary_language: ISO-639-3 language code for the primary language e.g. "eng". This is the language to
                             use for editing flows, not the language to be used in messaging.
    :type primary_language: str
    :param start_node: The start node in the flow graph. This node is visited first when a flow is triggered.
    :type start_node: FlowNode
    """

    def __init__(self, name, primary_language, start_node):
        self._uuid = generate_rapid_pro_uuid()
        self._name = name
        self._primary_language = primary_language
        self._start_node = start_node

    def get_nodes(self):
        nodes = dict()
        nodes_to_process = [self._start_node]
        while len(nodes_to_process) > 0:
            next_node = nodes_to_process.pop()
            if next_node is None:
                continue

            if next_node.uuid in nodes:
                continue

            nodes[next_node.uuid] = next_node
            nodes_to_process.extend(next_node.exits())

        return list(nodes.values())

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
            "nodes": [node.to_rapid_pro_dict() for node in self.get_nodes()],
        }


class FlowNode(abc.ABC):
    """
    Represents an abstract node in a flow graph.

    :param default_exit: The node to go to after this one, in the default case.
                         All FlowNodes have a default exit; some may define additional, conditional exits too.
                         If None, this FlowNode is at the end of a branch and no additional nodes will be visited after
                         this one.
    :type default_exit: FlowNode | None
    """

    def __init__(self, default_exit=None):
        self.uuid = generate_rapid_pro_uuid()
        self._default_exit = default_exit

    def to_rapid_pro_dict(self):
        pass

    def exits(self):
        """
        :return: List of all the exits from this node.
        :rtype: list of (FlowNode | None)
        """
        return [self._default_exit]


class WaitForResponseNode(FlowNode):
    """
    Represents a Rapid Pro "wait for response" node.

    Note that not all options available in Rapid Pro are available here.

    :param result_name: Rapid Pro flow variable to save the response text to.
                        This variable can be used to find the result in Rapid Pro exports and in its runs API.
    :type result_name: str
    :param opt_out_detectors: Opt out detectors to use to automatically identify an opt-out message.
                              If None, performs no automatic opt-out detection.
    :type opt_out_detectors: list of OptOutDetector | None
    :param opt_out_exit: Node to visit after this one if opt-out was detected by one of the `opt_out_detectors`.
    :type opt_out_exit: FlowNode | None
    :param default_exit: Node to visit after this one if no opt-out was detected.
    :type default_exit: FlowNode | None
    """

    def __init__(self, result_name, opt_out_detectors, opt_out_exit=None, default_exit=None):
        super().__init__(default_exit)
        self._result_name = result_name
        self._opt_out_detectors = opt_out_detectors
        self._opt_out_exit = opt_out_exit

    def to_rapid_pro_dict(self):
        opt_out_category_uuid = generate_rapid_pro_uuid()
        default_category_uuid = generate_rapid_pro_uuid()

        stop_exit_uuid = generate_rapid_pro_uuid()
        other_exit_uuid = generate_rapid_pro_uuid()

        return {
            "uuid": self.uuid,
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
                    "destination_uuid": None if self._opt_out_exit is None else self._opt_out_exit.uuid,
                    "uuid": stop_exit_uuid
                },
                {
                    "destination_uuid": None if self._default_exit is None else self._default_exit.uuid,
                    "uuid": other_exit_uuid
                }
            ],
            "actions": [],
        }

    def exits(self):
        return [self._opt_out_exit, self._default_exit]


class SendMessageNode(FlowNode):
    """
    Represents a Rapid Pro "Send Message" node.

    :param text: Text to send, in the flow's `default_language`.
    :type text: str
    :param default_exit: Node to visit after this one.
    :type default_exit: FlowNode | None
    """

    def __init__(self, text, default_exit=None):
        super().__init__(default_exit)
        self._text = text

    def to_rapid_pro_dict(self):
        return {
            "uuid": self.uuid,
            "actions": [
                {
                    "all_urns": False,
                    "attachments": [],
                    "quick_replies": [],
                    "text": self._text,
                    "type": "send_msg",
                    "uuid": generate_rapid_pro_uuid()
                }
            ],
            "exits": [
                {
                    "uuid": generate_rapid_pro_uuid(),
                    "destination_uuid": None if self._default_exit is None else self._default_exit.uuid
                }
            ]
        }


class ContactField:
    def __init__(self, key, name):
        self.key = key
        self.name = name


class SetContactFieldNode(FlowNode):
    def __init__(self, contact_field, value, default_exit=None):
        super().__init__(default_exit)
        self._contact_field = contact_field
        self._value = value

    def to_rapid_pro_dict(self):
        return {
            "uuid": self.uuid,
            "actions": [
                {
                    "uuid": generate_rapid_pro_uuid(),
                    "type": "set_contact_field",
                    "field": {
                        "key": self._contact_field.key,
                        "name": self._contact_field.name
                    },
                    "value": self._value
                }
            ],
            "exits": [
                {
                    "uuid": generate_rapid_pro_uuid(),
                    "destination_uuid": None if self._default_exit is None else self._default_exit.uuid
                }
            ]
        }


class ContactFieldHasTextSplitNode(FlowNode):
    def __init__(self, contact_field, has_text_exit=None, default_exit=None):
        super().__init__(default_exit)
        self._has_text_exit = has_text_exit
        self._contact_field = contact_field

    def to_rapid_pro_dict(self):
        has_text_category_uuid = generate_rapid_pro_uuid()
        other_category_uuid = generate_rapid_pro_uuid()

        has_text_exit_uuid = generate_rapid_pro_uuid()
        other_exit_uuid = generate_rapid_pro_uuid()

        return {
            "uuid": self.uuid,
            "actions": [],
            "router": {
                "default_category_uuid": other_category_uuid,
                "operand": f"@fields.{self._contact_field.key}",
                "type": "switch",
                "cases": [
                    {
                        "arguments": [],
                        "category_uuid": has_text_category_uuid,
                        "type": "has_text",
                        "uuid": generate_rapid_pro_uuid()
                    }
                ],
                "categories": [
                    {
                        "exit_uuid": has_text_exit_uuid,
                        "name": self._contact_field.name,
                        "uuid": has_text_category_uuid
                    },
                    {
                        "exit_uuid": other_exit_uuid,
                        "name": "Other",
                        "uuid": other_category_uuid
                    }
                ]
            },
            "exits": [
                {
                    "destination_uuid": None if self._has_text_exit is None else self._has_text_exit.uuid,
                    "uuid": has_text_exit_uuid
                },
                {
                    "destination_uuid": None if self._default_exit is None else self._default_exit.uuid,
                    "uuid": other_exit_uuid
                }
            ]
        }


class OptOutDetector(abc.ABC):
    def __init__(self):
        self._uuid = generate_rapid_pro_uuid()

    @abc.abstractmethod
    def to_rapid_pro_dict(self, category_uuid):
        pass


class RegexOptOutDetector(OptOutDetector):
    """
    Detects opt-outs using a regex.

    :param regex: Python regex to test responses against.
    :type regex: str
    """

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
    """
    Detects opt-outs by checking for an exact match with the test string.

    :param exact_phrase: Exact string to test responses against.
    :type exact_phrase: str
    """

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
