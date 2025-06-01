from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class JsonToCsvTransformerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(JsonToCsvTransformerOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # Your logic here
        print("Transforming JSON to CSV...")
        return True

class FileTransformerPlugin(AirflowPlugin):
    name = "file_transformer_plugin"
    operators = [JsonToCsvTransformerOperator]
