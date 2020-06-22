class MEDIA_TYPE(object):
    ALBUM = 'ALBUM'


class SQL(object):
    INSERT = '''
    INSERT	INTO {table_name}
            (
                REQUEST_PROVIDER
                ,REQUEST_ENDPOINT
                ,REQUEST_RESPONSE
                ,META_PAGE_NUMBER
                ,META_JOB_SUMMARY_SK
            )
    SELECT
            '{request_provider}'
            ,'{request_endpoint}'
            ,PARSE_JSON($${request_response}$$)
            ,{response_page}
            ,{job_id};'''


class STAGING_TABLE(object):
    REQUEST = 'REQUEST'


class WAREHOUSE_LAYER(object):
    STAGING = 'STAGING'
    MODEL = 'MODEL'
