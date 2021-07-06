from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to copy JSON data from AWS S3 to staging tables in Redshift
    :param       redshift_conn_id -- Redshift Connection ID
                 aws_credentials_id -- AWS Credentials (Secret Key & Access ID)
                 table -- Table to be copied to in redshift
                 s3_bucket -- S3 bucket to copy from
                 s3_key -- S3 path to copy from
                 json_option -- jsonpath file or auto for mapping columns in source and target table
                 aws_region -- region where S3 bucket resides
    """
    
    ui_color = '#358140'
    
    stage_sql_template= """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}'
        REGION AS '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_option="auto",
                 aws_region="us-west-2",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_option = json_option
        self.aws_region = aws_region
        

    def execute(self, context):
        self.log.info('StageToRedshiftOperator: Accessing credentials')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #self.log.info("Clearing data from destination Redshift table")
        #redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.stage_sql_template.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_option,
            self.aws_region
        )
        
        redshift.run(formatted_sql)





