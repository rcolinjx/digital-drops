from app_elt import shared
import requests


class TMDB:
    name = 'TMDB'
    type = 'FILM'
    conn = None
    api_key = None
    table_stage = None
    table_model = None

    def extract_load_upcoming(self):
        params = ['language=EN', 'region=US']
        url = f'''https://api.themoviedb.org/3/movie/upcoming?{'&'.join(params)}'''
        page_num = 1

        res_json = self.get_response(url, page_num)

        page_max = int(res_json['total_pages'])

        if page_max > 1:

            while page_num < page_max:
                page_num += 1
                self.get_response(url, page_num)

        self.conn.update_recent_requests(self.table_stage, self.name)

    def get_response(self, url, page):
        req_no_key = f'''{url}&page={str(page)}&api_key='''

        req = requests.get(req_no_key + self.api_key)

        res_json = req.json()

        if not res_json['results']:
            raise Exception(res_json['status_message'])

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
                                FROM    {self.table_stage} T
                                        ,LATERAL FLATTEN (INPUT => REQUEST_RESPONSE:"results")
                                WHERE   META_CURRENT_INDICATOR = TRUE
                                        AND REQUEST_PROVIDER = '{self.name}'
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
                                ,{self.conn.job_id}
                            )
        ''')

    def __init__(self, conn):
        self.api_key = shared.get_connection_keys(f'''key_api_{self.type}_{self.name}'''.lower())
        self.conn = conn
        self.table_stage = conn.tbl_stg
        self.table_model = '.'.join([conn.db, 'MODEL', self.type])
