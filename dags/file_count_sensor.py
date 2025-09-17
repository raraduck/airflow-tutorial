from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import os
import re

class FileCountSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, directory, file_regex, min_count=1, *args, **kwargs):
        super(FileCountSensor, self).__init__(*args, **kwargs)
        self.directory = directory
        self.file_regex = file_regex
        self.min_count = min_count

    def poke(self, context):
        self.log.info("Checking directory %s for files matching %s", self.directory, self.file_regex)
        try:
            files = os.listdir(self.directory)
            matched_files = [f for f in files if re.search(self.file_regex, f)]
            self.log.info("Found %d matching files", len(matched_files))
            return len(matched_files) >= self.min_count
        except Exception as e:
            self.log.error("Error reading directory: %s", e)
            return False
