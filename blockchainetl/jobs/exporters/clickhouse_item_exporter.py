import collections

from clickhouse_driver import Client

from blockchainetl.jobs.exporters.converters.composite_item_converter import \
  CompositeItemConverter
from blockchainetl.jobs.exporters.converters.simple_item_converter import \
  SimpleItemConverter


class ClickhouseItemExporter:
  clients: list[Client]
  converter: CompositeItemConverter
  partitioning_key: dict[str, str]
  suffix: str = None

  def __init__(
      self,
      connection_urls: list[str],
      converters: list[SimpleItemConverter],
      partitioning_key: dict[str, str],
      **kwargs
  ):
    self.clients = [Client.from_url(url) for url in connection_urls]
    self.converter = CompositeItemConverter(converters)
    self.partitioning_key = partitioning_key
    self.suffix = kwargs.get("suffix")

  def open(self):
    pass

  def export_items(self, items):
    items_grouped_by_type = group_by_item_type(items)

    for item_type, item_group in items_grouped_by_type.items():
      table_name = f'{item_type}s_{self.suffix}' if self.suffix is not None and len(self.suffix) > 0 else f'{item_type}s'

      partitioning_group = group_by_item_type(
        item_group,
        self.partitioning_key[item_type]
      )

      for index, rows in partitioning_group.items():
        client = self.clients[int(index) % len(self.clients)]
        output_result = [self.converter.convert_item(item) for item in rows]
        client.execute(f"INSERT INTO {table_name} VALUES", output_result)

  def close(self):
    pass


def group_by_item_type(items, key: str = 'type'):
  result = collections.defaultdict(list)
  for item in items:
    result[item.get(key)].append(item)

  return result
