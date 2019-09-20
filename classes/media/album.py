import requests


class Apple:
    name = 'APPLE'
    type = 'ALBUM'
    conn = None
    table_stage = None
    table_model = None

    def extract_load_upcoming(self):
        url = 'https://rss.itunes.apple.com/api/v1/us/apple-music/coming-soon/all/100/explicit.json'
        page_num = 1

        self.get_response(url, page_num)

        self.conn.update_recent_requests(self.table_stage, self.name)

    def get_response(self, url, page):
        req = requests.get(url)

        res_json = req.json()

        sql = f'''
                INSERT	INTO {self.table_stage}
                            (
                                REQUEST_PROVIDER
                                ,REQUEST_ENDPOINT
                                ,REQUEST_RESPONSE
                                ,META_PAGE_NUMBER
                                ,META_JOB_SUMMARY_SK
                            )
                SELECT	'{self.name}'
                        ,'{url}'
                        ,PARSE_JSON($${req.text}$$)
                        ,{page}
                        ,{self.conn.job_id};'''

        self.conn.cur.execute(sql)

        return res_json

    def transform_upcoming(self):
        # TODO: handle soft deletes?
        self.conn.cur.execute(f'''
            MERGE   INTO    {self.table_model} AS TARGET
                    USING   (
                                SELECT
                                        VALUE:"artistId" ARTIST_ID
                                        ,VALUE:"artistName" ARTIST_NAME
                                        ,VALUE:"artistUrl" ARTIST_URL
                                        ,VALUE:"artworkUrl100" ARTWORK_URL_100
                                        ,VALUE:"copyright" COPYRIGHT
                                        ,VALUE:"genres" GENRES
                                        ,VALUE:"id"::NUMBER(38,0) ID
                                        ,VALUE:"kind" KIND
                                        ,VALUE:"name" NAME
                                        ,VALUE:"releaseDate"::TIMESTAMP_NTZ RELEASE_DATE
                                        ,VALUE:"url" URL
                                        ,T.META_JOB_SUMMARY_SK
                                FROM    {self.table_stage} T
                                        ,LATERAL FLATTEN (INPUT => REQUEST_RESPONSE:"feed"."results")
                                WHERE   META_CURRENT_INDICATOR = TRUE
                                        AND REQUEST_PROVIDER = '{self.name}'
                            )   SOURCE
                            ON  TARGET.ALBUM_ID = SOURCE.ID

                    WHEN    MATCHED
                            AND NOT
                            (
                                TARGET.ALBUM_ARTIST_ID          =   SOURCE.ARTIST_ID
                                AND TARGET.ALBUM_ARTIST_NAME       =   SOURCE.ARTIST_NAME
                                AND TARGET.ALBUM_ARTIST_URL        =   SOURCE.ARTIST_URL
                                AND TARGET.ALBUM_ARTWORK_URL_100   =   SOURCE.ARTWORK_URL_100
                                AND TARGET.ALBUM_COPYRIGHT         =   SOURCE.COPYRIGHT
                                AND TARGET.ALBUM_GENRES            =   SOURCE.GENRES
                                AND TARGET.ALBUM_ID                =   SOURCE.ID
                                AND TARGET.ALBUM_KIND              =   SOURCE.KIND
                                AND TARGET.ALBUM_NAME              =   SOURCE.NAME
                                AND TARGET.ALBUM_RELEASE_DATE      =   SOURCE.RELEASE_DATE
                                AND TARGET.ALBUM_URL               =   SOURCE.URL
                            )
                    THEN    UPDATE
                    SET     ALBUM_ARTIST_ID             =   SOURCE.ARTIST_ID
                            ,ALBUM_ARTIST_NAME          =   SOURCE.ARTIST_NAME
                            ,ALBUM_ARTIST_URL           =   SOURCE.ARTIST_URL
                            ,ALBUM_ARTWORK_URL_100      =   SOURCE.ARTWORK_URL_100
                            ,ALBUM_COPYRIGHT            =   SOURCE.COPYRIGHT
                            ,ALBUM_GENRES               =   SOURCE.GENRES
                            ,ALBUM_ID                   =   SOURCE.ID
                            ,ALBUM_KIND                 =   SOURCE.KIND
                            ,ALBUM_NAME                 =   SOURCE.NAME
                            ,ALBUM_RELEASE_DATE         =   SOURCE.RELEASE_DATE
                            ,ALBUM_URL                  =   SOURCE.URL
                            ,META_UPDATE_TIMESTAMP_UTC    =   CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)::TIMESTAMP_NTZ

                    WHEN    NOT MATCHED
                    THEN    INSERT
                            (
                                ALBUM_ARTIST_ID
                                ,ALBUM_ARTIST_NAME
                                ,ALBUM_ARTIST_URL
                                ,ALBUM_ARTWORK_URL_100
                                ,ALBUM_COPYRIGHT
                                ,ALBUM_GENRES
                                ,ALBUM_ID
                                ,ALBUM_KIND
                                ,ALBUM_NAME
                                ,ALBUM_RELEASE_DATE
                                ,ALBUM_URL
                                ,META_JOB_SUMMARY_SK
                                ,META_REQUEST_SK
                            )
                            VALUES
                            (
                                SOURCE.ARTIST_ID
                                ,SOURCE.ARTIST_NAME
                                ,SOURCE.ARTIST_URL
                                ,SOURCE.ARTWORK_URL_100
                                ,SOURCE.COPYRIGHT
                                ,SOURCE.GENRES
                                ,SOURCE.ID
                                ,SOURCE.KIND
                                ,SOURCE.NAME
                                ,SOURCE.RELEASE_DATE
                                ,SOURCE.URL
                                ,SOURCE.META_JOB_SUMMARY_SK
                                ,{self.conn.job_id}
                            )
        ''')

    def __init__(self, conn):
        self.conn = conn
        self.table_stage = conn.tbl_stg
        self.table_model = '.'.join([conn.db, 'MODEL', self.type])
