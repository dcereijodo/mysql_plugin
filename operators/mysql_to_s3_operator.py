from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from mysql_plugin.hooks.astro_mysql_hook import AstroMySqlHook

from airflow.utils.decorators import apply_defaults
import json
import logging
import re


class MySQLToS3Operator(BaseOperator):
    """
    MySQL to Spreadsheet Operator

    NOTE: When using the MySQLToS3Operator, it is necessary to set the cursor
    to "dictcursor" in the MySQL connection settings within "Extra"
    (e.g.{"cursor":"dictcursor"}). To avoid invalid characters, it is also
    recommended to specify the character encoding (e.g {"charset":"utf8"}).

    NOTE: Because this operator accesses a single database via concurrent
    connections, it is advised that a connection pool be used to control
    requests. - https://airflow.incubator.apache.org/concepts.html#pools

    :param mysql_conn_id:           The input mysql connection id.
    :type mysql_conn_id:            string
    :param mysql_table:             The input MySQL table to pull data from.
    :type mysql_table:              string
    :param aws_conn_id:             The aws connection id.
    :type aws_conn_id:              string
    :param s3_bucket:               The destination s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The destination s3 key.
    :type s3_key:                   string
    :param package_schema:          *(optional)* Whether or not to pull the
                                    schema information for the table as well as
                                    the data.
    :type package_schema:           boolean
    :param target_db:               The db type the schema is generated for.
                                    Currently 'mysql' or 'redshift'
    :type target_db:                string
    :param incremental_key:         *(optional)* The incrementing key to filter
                                    the source data with. Currently only
                                    accepts a column with type of timestamp.
    :type incremental_key:          string
    :param start:                   *(optional)* The start date to filter
                                    records with based on the incremental_key.
                                    Only required if using the incremental_key
                                    field.
    :type start:                    timestamp (YYYY-MM-DD HH:MM:SS)
    :param end:                     *(optional)* The end date to filter
                                    records with based on the incremental_key.
                                    Only required if using the incremental_key
                                    field.
    :type end:                       timestamp (YYYY-MM-DD HH:MM:SS)
    """

    template_fields = ['start', 'end', 's3_key', 'query']

    @apply_defaults
    def __init__(self,
                 mysql_conn_id,
                 mysql_table,
                 aws_conn_id,
                 s3_bucket,
                 s3_key,
                 package_schema=False,
                 target_db='mysql',
                 query=None,
                 incremental_key=None,
                 start=None,
                 end=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.mysql_table = mysql_table
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.package_schema = package_schema
        self.target_db = target_db
        self.query = query
        self.incremental_key = incremental_key
        self.start = start
        self.end = end

    def execute(self, context):
        hook = AstroMySqlHook(self.mysql_conn_id)
        self.get_records(hook)
        if self.package_schema:
            self.get_schema(hook, self.mysql_table)

    def get_schema(self, hook, table):
        logging.info('Initiating schema retrieval.')
        results = list(hook.get_schema(table))
        output_array = []
        for i in results:
            new_dict = {}
            new_dict['name'] = i['COLUMN_NAME']
            new_dict['type'] = self.map_type(self.target_db, i['COLUMN_TYPE'])

            if len(new_dict) == 2:
                output_array.append(new_dict)
        self.s3_upload(json.dumps(output_array), schema=True)

    """
    Approximates MySQL data types to 'target_db' data types
    (It only supports some Redshift type conversions for the moment)
    """

    def map_type(self, target_db, type):
        if target_db == 'mysql':
            return type
        elif target_db == 'redshift':
            if type == 'DOUBLE':
                return 'DOUBLE PRECISION'
            if re.match(r"(TINY)?INT\([0-9]+\)", type, re.IGNORECASE):
                return 'INTEGER'
            if re.match(r"BIGINT\([0-9]+\)", type, re.IGNORECASE):
                return 'BIGINT'
            if re.match(r'MEDIUMTEXT', type, re.IGNORECASE) or re.match(r'LONGTEXT', type, re.IGNORECASE) or type.upper() == 'JSON':
                return 'VARCHAR(65535)'
            if re.match(r'DATETIME\([0-9]+\)', type, re.IGNORECASE):
                return 'TIMESTAMP'
            else:
                return type
        else:
            raise Exception("Unsupported target DB " + target_db)

    def get_records(self, hook):
        logging.info('Initiating record retrieval.')
        logging.info('Start Date: {0}'.format(self.start))
        logging.info('End Date: {0}'.format(self.end))

        if all([self.incremental_key, self.start, self.end]):
            query_filter = """ WHERE {0} >= '{1}' AND {0} < '{2}'
                """.format(self.incremental_key, self.start, self.end)

        if all([self.incremental_key, self.start]) and not self.end:
            query_filter = """ WHERE {0} >= '{1}'
                """.format(self.incremental_key, self.start)

        if not self.incremental_key:
            query_filter = ''

        if self.query is not None:
            query = \
                """
                SELECT *
                FROM (
                    {0}
                ) sq
                {1}
                """.format(self.query, query_filter)
        else:
            query = \
                """
                SELECT *
                FROM {0}
                {1}
                """.format(self.mysql_table, query_filter)
        logging.info('Query built for batch {}'.format(query))

        # Perform query and convert returned tuple to list
        results = list(hook.get_records(query))
        logging.info('Successfully performed query.')

        # Iterate through list of dictionaries (one dict per row queried)
        # and convert datetime and date values to isoformat.
        # (e.g. datetime(2017, 08, 01) --> "2017-08-01T00:00:00")
        results = [dict([k, str(v)] if v is not None else [k, v]
                   for k, v in i.items()) for i in results]
        results = '\n'.join([json.dumps(i) for i in results])
        self.s3_upload(results)
        return results

    def s3_upload(self, results, schema=False):
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        key = '{0}'.format(self.s3_key).rstrip()
        # If the file being uploaded to s3 is a schema, append "_schema" to the
        # end of the file name.
        logging.info('Uploading file {}'.format(key))
        logging.info('Schema flag is {}'.format(schema))
        if schema:
            key = key[:-5] + '_schema' + key[-5:]
        s3.load_string(
            string_data=results,
            bucket_name=self.s3_bucket,
            key=key,
            replace=True
        )
        logging.info('File uploaded to s3 key {}'.format(key))
