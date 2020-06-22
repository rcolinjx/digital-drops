import requests

from digital_drops.media.media import MediaProvider


class Tmdb(MediaProvider):
    PROVIDER = 'TMDB'
    TYPE = 'FILM'
    URL = 'https://api.themoviedb.org/3/movie/upcoming'

    def __init__(self, conn):
        super().__init__(conn, self.PROVIDER, self.TYPE)

    def _extract_from_api(self):
        params = ['language=EN', 'region=US']
        url = f'''{self.URL}?{'&'.join(params)}'''
        page_num = 1

        res_json = self._load_staging(url, page_num)

        page_max = self._get_page_from_response(res_json)
        if page_max > 1:
            while page_num <= page_max:
                page_num += 1
                res = self._load_staging(url, page_num)
                page_max = self._get_page_from_response(res)

    @staticmethod
    def _get_page_from_response(json) -> int:
        return int(json['total_pages'])

    def _load_staging(self, url: str, page: int):
        req_no_key = f'''{url}&page={str(page)}&api_key='''

        req = requests.get(req_no_key + self._api_key)

        res_json = req.json()

        if not res_json['results']:
            raise Exception(res_json['status_message'])

        sql = self.create_insert_query(url, req.text, page)

        print(sql)

        self._dao.cursor.execute(sql)

        return res_json

    def transform(self):
        # TODO: handle soft deletes?
        self._dao.cursor.execute(f'''
            MERGE   INTO    {self._target_table} AS TARGET
                    USING   (
                                SELECT
                                        VALUE:"adult"::BOOLEAN ADULT
                                        ,VALUE:"backdrop_path" BACKDROP_PATH
                                        ,VALUE:"genre_ids"::ARRAY GENRE_IDS
                                        ,VALUE:"id"::NUMBER(38,0) ID
                                        ,VALUE:"original_language" ORIGINAL_LANGUAGE
                                        ,VALUE:"original_title" ORIGINAL_TITLE
                                        ,VALUE:"overview" OVERVIEW
                                        ,VALUE:"popularity"::NUMBER(38,0) POPULARITY
                                        ,VALUE:"poster_path" POSTER_PATH
                                        ,VALUE:"release_date"::DATE RELEASE_DATE
                                        ,VALUE:"title" TITLE
                                        ,VALUE:"video"::BOOLEAN VIDEO
                                        ,VALUE:"vote_average"::NUMBER(38,3) VOTE_AVERAGE
                                        ,VALUE:"vote_count"::NUMBER(38,0) VOTE_COUNT
                                        ,T.META_JOB_SUMMARY_SK
                                FROM    {self._dao.staging_table} T
                                        ,LATERAL FLATTEN (INPUT => REQUEST_RESPONSE:"results")
                                WHERE   META_CURRENT_INDICATOR = TRUE
                                        AND REQUEST_PROVIDER = '{self._media_provider}'
                            )   SOURCE
                            ON  TARGET.FILM_ID = SOURCE.ID
            
                    WHEN    MATCHED
                            AND NOT
                            (
                                    TARGET.FILM_ADULT              =  SOURCE.ADULT
                                AND TARGET.FILM_BACKDROP_PATH      =  SOURCE.BACKDROP_PATH
                                AND TARGET.FILM_GENRE_IDS          =  SOURCE.GENRE_IDS
                                AND TARGET.FILM_ORIGINAL_LANGUAGE  =  SOURCE.ORIGINAL_LANGUAGE
                                AND TARGET.FILM_ORIGINAL_TITLE     =  SOURCE.ORIGINAL_TITLE
                                AND TARGET.FILM_OVERVIEW           =  SOURCE.OVERVIEW
                                AND TARGET.FILM_POPULARITY         =  SOURCE.POPULARITY
                                AND TARGET.FILM_POSTER_PATH        =  SOURCE.POSTER_PATH
                                AND TARGET.FILM_RELEASE_DATE       =  SOURCE.RELEASE_DATE
                                AND TARGET.FILM_TITLE              =  SOURCE.TITLE
                                AND TARGET.FILM_VIDEO              =  SOURCE.VIDEO
                                AND TARGET.FILM_VOTE_AVERAGE       =  SOURCE.VOTE_AVERAGE
                                AND TARGET.FILM_VOTE_COUNT         =  SOURCE.VOTE_COUNT
                            )
                    THEN    UPDATE
                    SET     FILM_ADULT                   =   SOURCE.ADULT
                            ,FILM_BACKDROP_PATH           =   SOURCE.BACKDROP_PATH
                            ,FILM_GENRE_IDS               =   SOURCE.GENRE_IDS
                            ,FILM_ORIGINAL_LANGUAGE       =   SOURCE.ORIGINAL_LANGUAGE
                            ,FILM_ORIGINAL_TITLE          =   SOURCE.ORIGINAL_TITLE
                            ,FILM_OVERVIEW                =   SOURCE.OVERVIEW
                            ,FILM_POPULARITY              =   SOURCE.POPULARITY
                            ,FILM_POSTER_PATH             =   SOURCE.POSTER_PATH
                            ,FILM_RELEASE_DATE            =   SOURCE.RELEASE_DATE
                            ,FILM_TITLE                   =   SOURCE.TITLE
                            ,FILM_VIDEO                   =   SOURCE.VIDEO
                            ,FILM_VOTE_AVERAGE            =   SOURCE.VOTE_AVERAGE
                            ,FILM_VOTE_COUNT              =   SOURCE.VOTE_COUNT
                            ,META_UPDATE_TIMESTAMP_UTC    =   CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)::TIMESTAMP_NTZ
                    
                    WHEN    NOT MATCHED
                    THEN    INSERT
                            (
                                FILM_ADULT
                                ,FILM_BACKDROP_PATH
                                ,FILM_GENRE_IDS
                                ,FILM_ID
                                ,FILM_ORIGINAL_LANGUAGE
                                ,FILM_ORIGINAL_TITLE
                                ,FILM_OVERVIEW
                                ,FILM_POPULARITY
                                ,FILM_POSTER_PATH
                                ,FILM_RELEASE_DATE
                                ,FILM_TITLE
                                ,FILM_VIDEO
                                ,FILM_VOTE_AVERAGE
                                ,FILM_VOTE_COUNT
                                ,META_JOB_SUMMARY_SK
                                ,META_REQUEST_SK
                            )
                            VALUES
                            (
                                SOURCE.ADULT
                                ,SOURCE.BACKDROP_PATH
                                ,SOURCE.GENRE_IDS
                                ,SOURCE.ID
                                ,SOURCE.ORIGINAL_LANGUAGE
                                ,SOURCE.ORIGINAL_TITLE
                                ,SOURCE.OVERVIEW
                                ,SOURCE.POPULARITY
                                ,SOURCE.POSTER_PATH
                                ,SOURCE.RELEASE_DATE
                                ,SOURCE.TITLE
                                ,SOURCE.VIDEO
                                ,SOURCE.VOTE_AVERAGE
                                ,SOURCE.VOTE_COUNT
                                ,SOURCE.META_JOB_SUMMARY_SK
                                ,{self._dao.job_id}
                            )
        ''')
