import json
from medium import film
import snowflake.connector


def main():

    conn_key = get_connection_keys('db_connection')

    conn_key_database = conn_key['database']
    conn_key_schema = 'STAGING'

    conn = snowflake.connector.connect(
        user=conn_key['user'],
        password=conn_key['password'],
        account=conn_key['account'],
        warehouse=conn_key['warehouse'],
        database=conn_key_database,
        schema=conn_key_schema
    )

    cur = conn.cursor()

    # JOB AUDIT
    job_id = get_job_id(cur, conn_key_database)

    # FILM - TMDB
    tmdb_api_key = common.get_keys('key_api_film_tmdb')
    tmdb_table = '.'.join([conn_key_database, conn_key_schema, 'REQUEST'])
    tmdb = film.TMDB(tmdb_api_key, tmdb_table, job_id)
    tmdb.extract_load_upcoming(cur)


def get_connection_keys(media_key):
    with open('./config.json') as file_json_config:
        config = json.load(file_json_config)

    return config[media_key]


def get_job_id(cur, database):
    cur.execute("INSERT INTO {0}.CONTROL.JOB_SUMMARY(JOB_SUMMARY_STATUS) SELECT 'RUNNING';".format(database))
    cur.execute('SELECT MAX(JOB_SUMMARY_SK) FROM {0}.CONTROL.JOB_SUMMARY AT(STATEMENT=>LAST_QUERY_ID());'
                .format(database))

    job_id = cur.fetchone()[0]

    return job_id

if __name__ == "__main__":
    main()
