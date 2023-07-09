import json

from blockchainetl.jobs.exporters.converters.simple_item_converter import \
  SimpleItemConverter


class ObjectToJsonFieldItemConverter(SimpleItemConverter):

  def __init__(self, keys: list[str]):
    self.keys = keys

  def convert_field(self, key, value):
    if (isinstance(value, dict) or isinstance(value, list)) and (
        self.keys is None or key in self.keys):
      return json.dumps(value)
    else:
      return value
