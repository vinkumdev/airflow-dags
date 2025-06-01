import requests
import csv
import os
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

class JsonToCsvTransformerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, url, output_path, extra_column_name="source", extra_column_value="api", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.output_path = output_path
        self.extra_column_name = extra_column_name
        self.extra_column_value = extra_column_value

    def execute(self, context):
        self.log.info(f"Fetching data from {self.url}")
        response = requests.get(self.url)
        response.raise_for_status()
        data = response.json()

        # Add the extra column
        data[self.extra_column_name] = self.extra_column_value

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

        # Save to CSV
        with open(self.output_path, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=data.keys())
            writer.writeheader()
            writer.writerow(data)

        self.log.info(f"Saved transformed data to {self.output_path}")

# Plugin class
class FileTransformerPlugin(AirflowPlugin):
    name = "file_transformer_plugin"
    operators = [JsonToCsvTransformerOperator]
