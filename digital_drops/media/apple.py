import requests

from digital_drops.media.media import MediaProvider


class Apple(MediaProvider):
    PROVIDER = 'APPLE'
    TYPE = 'ALBUM'
    URL = 'https://rss.itunes.apple.com/api/v1/us/apple-music/coming-soon/all/100/explicit.json'

    def __init__(self, conn):
        super().__init__(conn, self.PROVIDER, self.TYPE)

    def _extract_from_api(self):
        url = self.URL
        page_num = 1

        self._load_staging(url, page_num)

    def _load_staging(self, url, page):
        req = requests.get(url)

        res_json = req.json()

        sql = self.create_insert_query(url, req.text, page)

        self._dao.cursor.execute(sql)

        return res_json

    def _transform(self):
        # TODO: handle soft deletes?
        self._dao.cursor.execute(f'''
            MERGE   INTO    {self._target_table} AS TARGET
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
                                FROM    {self._dao.staging_table} T
                                        ,LATERAL FLATTEN (INPUT => REQUEST_RESPONSE:"feed"."results")
                                WHERE   META_CURRENT_INDICATOR = TRUE
                                        AND REQUEST_PROVIDER = '{self._media_provider}'
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
                                ,{self._dao.job_id}
                            )
        ''')
