from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator for data quality checks on the dimension tables by checking for null
    columns
    :param      tables -- dictionary with table name and the column to check for null value
                redshift_conn_id -- Redshift connection Id
    """
    
    ui_color = '#89DA59'
    
    test_sql_template="""
    SELECT
        COUNT(*)
    FROM {test_table}
    WHERE {test_column} IS NULL;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
       
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator: establishing connection to Redshift')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Running quality check')
        
            
        for key,value in self.tables.items():
            test_sql=DataQualityOperator.test_sql_template.format(test_table=key,test_column=value)
            test_table=key
            test_column=value
            #results=redshift.run(test_sql)
            result=redshift.get_records(test_sql)
            
            if result[0][0] is 0:
                self.log.info(f'Data quality passed for {test_table}')
            else:
                raise ValueError(f'Data quality failed for {test_table}')
                    
                 
            
           
                
            
            