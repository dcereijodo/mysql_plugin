from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftToMySqlTransfer(BaseOperator):
    """
    Moves data from Redshift to MySQL, note that for now the data is loaded
    into memory before being pushed to MySQL, so this operator should
    be used for smallish amount of data.

    :param sql: SQL query to execute against Redshift. (templated)
    :type sql: str
    :param mysql_table: target MySQL table, use dot notation to target a
        specific database. (templated)
    :type mysql_table: str
    :param mysql_conn_id: source mysql connection
    :type mysql_conn_id: str
    :param redshift_conn_id: source redshift connection
    :type redshift_conn_id: str
    :param mysql_preoperator: sql statement to run against mysql prior to
        import, typically use to truncate of delete in place
        of the data coming in, allowing the task to be idempotent (running
        the task twice won't double load data). (templated)
    :type mysql_preoperator: str
    """

    template_fields = ('sql', 'mysql_table', 'mysql_preoperator')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 sql,
                 mysql_table,
                 redshift_conn_id,
                 mysql_conn_id='mysql_default',
                 mysql_preoperator=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.mysql_table = mysql_table
        self.mysql_conn_id = mysql_conn_id
        self.mysql_preoperator = mysql_preoperator
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Extracting data from Redshift: %s", self.sql)
        results = postgres.get_records(self.sql)

        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        if self.mysql_preoperator:
            self.log.info("Running MySQL preoperator")
            self.log.info(self.mysql_preoperator)
            mysql.run(self.mysql_preoperator)

        self.log.info("Inserting rows into MySQL")
        mysql.insert_rows(table=self.mysql_table, rows=results)
