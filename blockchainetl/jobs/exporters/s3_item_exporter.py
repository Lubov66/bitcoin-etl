import logging
import os
from pathlib import Path

import boto3

from blockchainetl.file_utils import close_silently
from blockchainetl.jobs.exporters.composite_item_exporter import \
  CompositeItemExporter

logger = logging.getLogger('S3ItemExporter')


class S3ItemExporter(CompositeItemExporter):
  directory: str = '/'

  def __init__(
      self,
      output: str,
      filename_mapping,
      field_mapping=None,
      converters=(),
  ):
    super().__init__(filename_mapping, field_mapping, converters=converters)
    self.field_mapping = {}
    self.file_mapping = {}
    self.exporter_mapping = {}
    self.counter_mapping = {}
    self.s3 = boto3.resource('s3')
    path = output.replace('s3://', '')
    self.directory = path[path.index('/') + 1:]
    self.bucket = self.s3.Bucket(path[:path.index('/')])

  def close(self):
    for item_type, file in self.file_mapping.items():
      counter = self.counter_mapping[item_type]
      close_silently(file)
      if counter is not None:
        self.logger.info(
            '{} items exported: {}'.format(item_type, counter.increment() - 1))

        file_path = Path(file.name)
        upload_file = os.path.join(self.directory, file_path.name)
        self.bucket.upload_file(file.name, upload_file)
        self.logger.info('Uploaded {} to S3'.format(item_type))

        os.remove(file.name)
        os.rmdir(file_path.parents[0])
