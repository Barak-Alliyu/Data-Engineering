from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator to copy dimension tables from staging tables
    :params     redshift_conn_id -- Redshift Connection ID
                dim_sql_query -- SQL query to insert data in dimention tables
                table -- Target table to be filled
                clear_table -- Option to select whether to clear table data before insert
                                Defaults to True
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 dim_sql_query="",
                 table="",
                 clear_table=True,
                 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.dim_sql_query= dim_sql_query
        self.table= table
        self.clear_table= clear_table

    def execute(self, context):
        self.log.info('LoadDimensionOperator: establishing connection to Redshift')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.clear_table:
            self.log.info("Deleting data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
        else:
            pass
        

        self.log.info("Copying data from staging tables to dimension table")
        
        redshift.run(self.dim_sql_query)
