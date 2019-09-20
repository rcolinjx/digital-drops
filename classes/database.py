from app_elt import shared
import snowflake.connector


class SnowflakeConnection:
    db = None
    cur = None
    job_id = None
    tbl_stg = None

    def get_job_id(self):
        self.cur.execute(f'''
            INSERT  INTO {self.db}.CONTROL.JOB_SUMMARY(JOB_SUMMARY_STATUS)
            SELECT  'RUNNING';''')
        self.cur.execute(f'''
                SELECT  MAX(JOB_SUMMARY_SK)
                FROM    {self.db}.CONTROL.JOB_SUMMARY AT(STATEMENT=>LAST_QUERY_ID());''')

        job_id = self.cur.fetchone()[0]

        return job_id

    def update_recent_requests(self, table, provider):
        self.cur.execute('BEGIN;')
        self.cur.execute(f'''
            UPDATE  {table}
            SET     META_CURRENT_INDICATOR = FALSE
            WHERE   REQUEST_PROVIDER = '{provider}';''')
        self.cur.execute(f'''
            UPDATE  {table}
            SET     META_CURRENT_INDICATOR = TRUE
            WHERE   REQUEST_PROVIDER = '{provider}'
                    AND META_JOB_SUMMARY_SK = {self.job_id};''')
        self.cur.execute('COMMIT;')

    def __init__(self, key_name):
        conn_key = shared.get_connection_keys(key_name)

        self.db = conn_key['database']
        self.tbl_stg = '.'.join([self.db, 'STAGING', 'REQUEST'])

        self.cur = snowflake.connector.connect(
            user=conn_key['user'],
            password=conn_key['password'],
            account=conn_key['account'],
            warehouse=conn_key['warehouse'],
            database=self.db,
        ).cursor()

        self.job_id = self.get_job_id()
