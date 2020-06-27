import snowflake.connector

from digital_drops import util
from digital_drops.constants import WAREHOUSE_LAYER, STAGING_TABLE


class SnowflakeDao(object):
    def __init__(self):
        conn_key = util.get_connection_keys('database_connection')

        self.db = conn_key['database']
        self.staging_table = '{}.{}.{}'.format(self.db, WAREHOUSE_LAYER.STAGING, STAGING_TABLE.REQUEST)

        self.cursor = snowflake.connector.connect(
            user=conn_key['user'],
            password=conn_key['password'],
            account=conn_key['account'],
            warehouse=conn_key['warehouse'],
            database=self.db,
        ).cursor()

        self.job_id = self._get_job_id()

    def _get_job_id(self) -> str:
        self.cursor.execute(f'''
            INSERT  INTO {self.db}.CONTROL.JOB_SUMMARY(JOB_SUMMARY_STATUS)
            SELECT  'RUNNING';''')
        self.cursor.execute(f'''
                SELECT  MAX(JOB_SUMMARY_SK)
                FROM    {self.db}.CONTROL.JOB_SUMMARY AT(STATEMENT=>LAST_QUERY_ID());''')

        return self.cursor.fetchone()[0]

    def update_recent_requests(self, provider: str):
        self.cursor.execute('BEGIN;')
        self.cursor.execute(f'''
            UPDATE  {self.staging_table}
            SET     META_CURRENT_INDICATOR = FALSE
            WHERE   REQUEST_PROVIDER = '{provider}';''')
        self.cursor.execute(f'''
            UPDATE  {self.staging_table}
            SET     META_CURRENT_INDICATOR = TRUE
            WHERE   REQUEST_PROVIDER = '{provider}'
                    AND META_JOB_SUMMARY_SK = {self.job_id};''')
        self.cursor.execute('COMMIT;')

        # self.job_id = self._get_job_id()

    def get_target_table(self, table_type: str) -> str:
        return '.'.join([self.db, WAREHOUSE_LAYER.MODEL, table_type])
