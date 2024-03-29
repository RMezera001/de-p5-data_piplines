from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 sql_create = '',
                 sql_insert = '',
                 append_data = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_create = sql_create
        self.sql_insert = sql_insert
        self.append_data = append_data

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Creating {self.table} table")
        redshift.run(self.sql_create)          
        
        self.log.info(f"Inserting data into {self.table} table")
        if self.append_data == True:
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql_insert)
            redshift.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table
            redshift.run(sql_statement)

            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql_insert)
            redshift.run(sql_statement)       