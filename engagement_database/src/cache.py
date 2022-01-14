import json

from core_data_modules.util import IOUtils


class Cache:
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir

    def set_doc(self, entry_name, doc):
        export_path = f"{self.cache_dir}/{entry_name}.json"
        IOUtils.ensure_dirs_exist_for_file(export_path)
        with open(export_path, "w") as f:
            json.dump(doc.to_dict(serialize_datetimes_to_str=True), f)

    def get_doc(self, entry_name, doc_type):
        try:
            with open(f"{self.cache_dir}/{entry_name}.json") as f:
                return doc_type.from_dict(json.load(f))
        except FileNotFoundError:
            return None
