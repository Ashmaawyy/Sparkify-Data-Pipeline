from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow import DAG

class LoadFactsOperator(BaseOperator):

    ui_color = '#F98866'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 task_id = "task_id",
                 dag = DAG,
                 *args, **kwargs):

        super(LoadFactsOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
