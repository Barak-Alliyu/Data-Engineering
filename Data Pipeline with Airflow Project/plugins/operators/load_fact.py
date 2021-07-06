from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Operator to copy fact table from staging tables
    :params     redshift_conn_id -- Redshift Connection ID
                fact_sql_query -- SQL query to insert data in fact tables
    """
    
    ui_color = '#F98866'
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_sql_query= fact_sql_query

    def execute(self, context):
        self.log.info('LoadFactOperator: establishing connection to Redshift')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Copying data from staging tables to fact table")
        
        
        redshift.run(self.fact_sql_query)
