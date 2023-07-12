import logging
import os

from blockchainetl.file_utils import smart_open


class FileCheckpoint:
  last_synced_block_file: str

  def __init__(self, last_synced_block_file):
    self.last_synced_block_file = last_synced_block_file

  def write_last_synced_block(self, last_synced_block):
    self.write_to_file(str(last_synced_block) + '\n')

  def init_last_synced_block_file(self, start_block):
    if os.path.isfile(self.last_synced_block_file):
      raise ValueError(
          '{} should not exist if --start-block option is specified. '
          'Either remove the {} file or the --start-block option.'
          .format(self.last_synced_block_file, self.last_synced_block_file))
    self.write_last_synced_block(start_block)

  def read_last_synced_block(self, default_value):
    if self.last_synced_block_file is None or not os.path.isfile(self.last_synced_block_file):
      return default_value
    with smart_open(self.last_synced_block_file, 'r') as last_synced_block_file:
      point = last_synced_block_file.read()
      logging.info(f'Load checkpoint {point}')
      return int(point)

  def write_to_file(self, content):
    if self.last_synced_block_file is None:
      return
    with smart_open(self.last_synced_block_file, 'w') as file_handle:
      file_handle.write(content)
