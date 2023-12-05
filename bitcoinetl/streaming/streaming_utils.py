from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.converters.object_to_json_field_item_converter import \
  ObjectToJsonFieldItemConverter


def get_item_exporter(output):
    item_exporter_type = determine_item_exporter_type(output)
    if item_exporter_type == ItemExporterType.PUBSUB:
        from blockchainetl.jobs.exporters.google_pubsub_item_exporter import GooglePubSubItemExporter
        item_exporter = GooglePubSubItemExporter(
            item_type_to_topic_mapping={
                'block': output + '.blocks',
                'transaction': output + '.transactions'
            },
            message_attributes=('item_id',))
    elif item_exporter_type == ItemExporterType.CLICKHOUSE:
        from blockchainetl.jobs.exporters.clickhouse_item_exporter import ClickhouseItemExporter
        item_exporter = ClickhouseItemExporter(
            connection_urls=output.split(','),
            converters=[ObjectToJsonFieldItemConverter(keys=['inputs', 'outputs'])],
            partitioning_key={
                'block': 'number',
                'transaction': 'block_number'
            }
        )
    elif item_exporter_type == ItemExporterType.KAFKA:
        from blockchainetl.jobs.exporters.kafka_item_exporter import KafkaItemExporter
        item_exporter = KafkaItemExporter(
            output=output,
            item_type_to_topic_mapping={
                'block': 'blocks',
                'transaction': 'transactions',
            },
            converters=[ObjectToJsonFieldItemConverter(keys=['inputs', 'outputs'])]
        )
    else:
        item_exporter = ConsoleItemExporter()

    return item_exporter



def determine_item_exporter_type(output):
    if output is not None and output.startswith('projects'):
        return ItemExporterType.PUBSUB
    elif output is not None and output.startswith('clickhouse'):
        return ItemExporterType.CLICKHOUSE
    elif output is not None and output.startswith('s3'):
        return ItemExporterType.S3
    elif output is not None and output.startswith('kafka'):
        return ItemExporterType.KAFKA


class ItemExporterType:
  PUBSUB = 'pubsub'
  CLICKHOUSE = 'clickhouse'
  KAFKA = 'kafka'
  S3 = 'clickhouse'
