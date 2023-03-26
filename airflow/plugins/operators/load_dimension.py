from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table="",
                 redshift_conn_id="",
                 sql_query="",
                 append_mode=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.append_mode = append_mode

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')

        self.log.info("Initializing AWS credentials for loading data into dimension table: {}".format(self.table))
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_mode:
            self.log.info("Appending data to destination Redshift table: {}".format(self.table))
        else:
            self.log.info("Deleting data from destination Redshift table: {}".format(self.table))
            redshift_hook.run("DELETE FROM {}".format(self.table))

        self.log.info("Executing SQL query to load data into dimension table: {}".format(self.table))
        load_dimension = "INSERT INTO {} {}".format(self.table, self.sql_query)
        redshift_hook.run(load_dimension)