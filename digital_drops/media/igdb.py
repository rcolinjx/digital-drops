import time

import requests

from digital_drops.media.media import MediaProvider


class Igdb(MediaProvider):
    PROVIDER = 'IGDB'
    TYPE = 'GAME'
    URL = 'https://api-v3.igdb.com/games/'

    def __init__(self, conn):
        super().__init__(conn, self.PROVIDER, self.TYPE)

    def _extract_from_api(self):
        url = self.URL
        header = {'user-key': self._api_key}
        epoch_ms = int(round(time.time() * 1000))
        page_num = 1
        offset = 0
        limit = 50
        res_continue = True

        # TODO: debug results
        while res_continue:
            body = f'''
                fields *;
                where first_release_date > {epoch_ms};
                sort date asc;
                offset {offset * limit};
                limit {limit};'''

            res_continue = self._load_staging(url, header, body, page_num)

            page_num += 1
            offset += 1

    def _load_staging(self, url: str, header: dict, body: str, page: int):
        req = requests.post(url, data=body, headers=header)

        res_json = req.json()

        if "cause" in res_json[0]:
            raise Exception(body)

        sql = self.create_insert_query(url, req.text, page)

        self._dao.cursor.execute(sql)

    def _transform(self):
        # TODO: handle soft deletes?
        self._dao.cursor.execute(f'''
            MERGE   INTO    {self._target_table} AS TARGET
                    USING   (
                                SELECT
                                        GET(VALUE,'age_ratings')::ARRAY AGE_RATINGS
                                        ,GET(VALUE,'aggregated_rating')::NUMBER(38,15) AGGREGATED_RATING
                                        ,GET(VALUE,'aggregated_rating_count')::NUMBER(38,0) AGGREGATED_RATING_COUNT
                                        ,GET(VALUE,'alternative_names')::ARRAY ALTERNATIVE_NAMES
                                        ,GET(VALUE,'artworks')::ARRAY ARTWORKS
                                        ,GET(VALUE,'bundles')::ARRAY BUNDLES
                                        ,GET(VALUE,'category')::NUMBER(38,0) CATEGORY
                                        ,GET(VALUE,'collection')::NUMBER(38,0) COLLECTION
                                        ,GET(VALUE,'cover')::VARCHAR COVER
                                        ,GET(VALUE,'created_at')::timestamp_ntz CREATED_AT
                                        ,GET(VALUE,'dlcs')::ARRAY DLCS
                                        ,GET(VALUE,'expansions')::ARRAY EXPANSIONS
                                        ,GET(VALUE,'external_games')::ARRAY EXTERNAL_GAMES
                                        ,GET(VALUE,'first_release_date')::timestamp_ntz FIRST_RELEASE_DATE
                                        ,GET(VALUE,'follows')::NUMBER(38,0) FOLLOWS
                                        ,GET(VALUE,'franchise')::NUMBER(38,0) FRANCHISE
                                        ,GET(VALUE,'franchises')::ARRAY FRANCHISES
                                        ,GET(VALUE,'game_engines')::ARRAY GAME_ENGINES
                                        ,GET(VALUE,'game_modes')::ARRAY GAME_MODES
                                        ,GET(VALUE,'genres')::ARRAY GENRES
                                        ,GET(VALUE,'hypes')::NUMBER(38,0) HYPES
                                        ,GET(VALUE,'id')::NUMBER(38,0) ID
                                        ,GET(VALUE,'involved_companies')::ARRAY INVOLVED_COMPANIES
                                        ,GET(VALUE,'keywords')::ARRAY KEYWORDS
                                        ,GET(VALUE,'multiplayer_modes')::ARRAY MULTIPLAYER_MODES
                                        ,GET(VALUE,'name')::VARCHAR NAME
                                        ,GET(VALUE,'parent_game')::NUMBER(38,0) PARENT_GAME
                                        ,GET(VALUE,'platforms')::ARRAY PLATFORMS
                                        ,GET(VALUE,'player_perspectives')::ARRAY PLAYER_PERSPECTIVES
                                        ,GET(VALUE,'popularity')::NUMBER(38,15) POPULARITY
                                        ,GET(VALUE,'pulse_count')::NUMBER(38,0) PULSE_COUNT
                                        ,GET(VALUE,'rating')::NUMBER(38,15) RATING
                                        ,GET(VALUE,'rating_count')::NUMBER(38,0) RATING_COUNT
                                        ,GET(VALUE,'release_dates')::ARRAY RELEASE_DATES
                                        ,GET(VALUE,'screenshots')::ARRAY SCREENSHOTS
                                        ,GET(VALUE,'similar_games')::ARRAY SIMILAR_GAMES
                                        ,GET(VALUE,'slug')::VARCHAR SLUG
                                        ,GET(VALUE,'standalone_expansions')::ARRAY STANDALONE_EXPANSIONS
                                        ,GET(VALUE,'status')::NUMBER(38,0) STATUS
                                        ,GET(VALUE,'storyline')::VARCHAR STORYLINE
                                        ,GET(VALUE,'summary')::VARCHAR SUMMARY
                                        ,GET(VALUE,'tags')::ARRAY TAGS
                                        ,GET(VALUE,'themes')::ARRAY THEMES
                                        ,GET(VALUE,'time_to_beat')::NUMBER(38,0) TIME_TO_BEAT
                                        ,GET(VALUE,'total_rating')::NUMBER(38,15) TOTAL_RATING
                                        ,GET(VALUE,'total_rating_count')::NUMBER(38,0) TOTAL_RATING_COUNT
                                        ,GET(VALUE,'updated_at')::timestamp_ntz UPDATED_AT
                                        ,GET(VALUE,'url')::VARCHAR URL
                                        ,GET(VALUE,'version_parent')::NUMBER(38,0) VERSION_PARENT
                                        ,GET(VALUE,'version_title')::VARCHAR VERSION_TITLE
                                        ,GET(VALUE,'videos')::ARRAY VIDEOS
                                        ,GET(VALUE,'websites')::ARRAY WEBSITES
                                        ,T.META_JOB_SUMMARY_SK
                                FROM    {self._dao.staging_table} T
                                        ,LATERAL FLATTEN (INPUT => REQUEST_RESPONSE)
                                WHERE   META_CURRENT_INDICATOR = TRUE
                                        AND REQUEST_PROVIDER = '{self._media_provider}'
                            )   SOURCE
                            ON  TARGET.GAME_ID = SOURCE.ID

                    WHEN    MATCHED
                            AND TARGET.GAME_UPDATED_AT <> SOURCE.UPDATED_AT
                    THEN    UPDATE
                    SET		GAME_AGE_RATINGS				=	SOURCE.AGE_RATINGS
                            ,GAME_AGGREGATED_RATING			=	SOURCE.AGGREGATED_RATING
                            ,GAME_AGGREGATED_RATING_COUNT	=	SOURCE.AGGREGATED_RATING_COUNT
                            ,GAME_ALTERNATIVE_NAMES			=	SOURCE.ALTERNATIVE_NAMES
                            ,GAME_ARTWORKS					=	SOURCE.ARTWORKS
                            ,GAME_BUNDLES					=	SOURCE.BUNDLES
                            ,GAME_CATEGORY					=	SOURCE.CATEGORY
                            ,GAME_COLLECTION				=	SOURCE.COLLECTION
                            ,GAME_COVER						=	SOURCE.COVER
                            ,GAME_CREATED_AT				=	SOURCE.CREATED_AT
                            ,GAME_DLCS						=	SOURCE.DLCS
                            ,GAME_EXPANSIONS				=	SOURCE.EXPANSIONS
                            ,GAME_EXTERNAL_GAMES			=	SOURCE.EXTERNAL_GAMES
                            ,GAME_FIRST_RELEASE_DATE		=	SOURCE.FIRST_RELEASE_DATE
                            ,GAME_FOLLOWS					=	SOURCE.FOLLOWS
                            ,GAME_FRANCHISE					=	SOURCE.FRANCHISE
                            ,GAME_FRANCHISES				=	SOURCE.FRANCHISES
                            ,GAME_GAME_ENGINES				=	SOURCE.GAME_ENGINES
                            ,GAME_GAME_MODES				=	SOURCE.GAME_MODES
                            ,GAME_GENRES					=	SOURCE.GENRES
                            ,GAME_HYPES						=	SOURCE.HYPES
                            ,GAME_ID						=	SOURCE.ID
                            ,GAME_INVOLVED_COMPANIES		=	SOURCE.INVOLVED_COMPANIES
                            ,GAME_KEYWORDS					=	SOURCE.KEYWORDS
                            ,GAME_MULTIPLAYER_MODES			=	SOURCE.MULTIPLAYER_MODES
                            ,GAME_NAME						=	SOURCE.NAME
                            ,GAME_PARENT_GAME				=	SOURCE.PARENT_GAME
                            ,GAME_PLATFORMS					=	SOURCE.PLATFORMS
                            ,GAME_PLAYER_PERSPECTIVES		=	SOURCE.PLAYER_PERSPECTIVES
                            ,GAME_POPULARITY				=	SOURCE.POPULARITY
                            ,GAME_PULSE_COUNT				=	SOURCE.PULSE_COUNT
                            ,GAME_RATING					=	SOURCE.RATING
                            ,GAME_RATING_COUNT				=	SOURCE.RATING_COUNT
                            ,GAME_RELEASE_DATES				=	SOURCE.RELEASE_DATES
                            ,GAME_SCREENSHOTS				=	SOURCE.SCREENSHOTS
                            ,GAME_SIMILAR_GAMES				=	SOURCE.SIMILAR_GAMES
                            ,GAME_SLUG						=	SOURCE.SLUG
                            ,GAME_STANDALONE_EXPANSIONS		=	SOURCE.STANDALONE_EXPANSIONS
                            ,GAME_STATUS					=	SOURCE.STATUS
                            ,GAME_STORYLINE					=	SOURCE.STORYLINE
                            ,GAME_SUMMARY					=	SOURCE.SUMMARY
                            ,GAME_TAGS						=	SOURCE.TAGS
                            ,GAME_THEMES					=	SOURCE.THEMES
                            ,GAME_TIME_TO_BEAT				=	SOURCE.TIME_TO_BEAT
                            ,GAME_TOTAL_RATING				=	SOURCE.TOTAL_RATING
                            ,GAME_TOTAL_RATING_COUNT		=	SOURCE.TOTAL_RATING_COUNT
                            ,GAME_UPDATED_AT				=	SOURCE.UPDATED_AT
                            ,GAME_URL						=	SOURCE.URL
                            ,GAME_VERSION_PARENT			=	SOURCE.VERSION_PARENT
                            ,GAME_VERSION_TITLE				=	SOURCE.VERSION_TITLE
                            ,GAME_VIDEOS					=	SOURCE.VIDEOS
                            ,GAME_WEBSITES					=	SOURCE.WEBSITES
                            ,META_UPDATE_TIMESTAMP_UTC		=   CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)::TIMESTAMP_NTZ

                    WHEN    NOT MATCHED
                    THEN    INSERT
                            (
                                GAME_AGE_RATINGS
                                ,GAME_AGGREGATED_RATING
                                ,GAME_AGGREGATED_RATING_COUNT
                                ,GAME_ALTERNATIVE_NAMES
                                ,GAME_ARTWORKS
                                ,GAME_BUNDLES
                                ,GAME_CATEGORY
                                ,GAME_COLLECTION
                                ,GAME_COVER
                                ,GAME_CREATED_AT
                                ,GAME_DLCS
                                ,GAME_EXPANSIONS
                                ,GAME_EXTERNAL_GAMES
                                ,GAME_FIRST_RELEASE_DATE
                                ,GAME_FOLLOWS
                                ,GAME_FRANCHISE
                                ,GAME_FRANCHISES
                                ,GAME_GAME_ENGINES
                                ,GAME_GAME_MODES
                                ,GAME_GENRES
                                ,GAME_HYPES
                                ,GAME_ID
                                ,GAME_INVOLVED_COMPANIES
                                ,GAME_KEYWORDS
                                ,GAME_MULTIPLAYER_MODES
                                ,GAME_NAME
                                ,GAME_PARENT_GAME
                                ,GAME_PLATFORMS
                                ,GAME_PLAYER_PERSPECTIVES
                                ,GAME_POPULARITY
                                ,GAME_PULSE_COUNT
                                ,GAME_RATING
                                ,GAME_RATING_COUNT
                                ,GAME_RELEASE_DATES
                                ,GAME_SCREENSHOTS
                                ,GAME_SIMILAR_GAMES
                                ,GAME_SLUG
                                ,GAME_STANDALONE_EXPANSIONS
                                ,GAME_STATUS
                                ,GAME_STORYLINE
                                ,GAME_SUMMARY
                                ,GAME_TAGS
                                ,GAME_THEMES
                                ,GAME_TIME_TO_BEAT
                                ,GAME_TOTAL_RATING
                                ,GAME_TOTAL_RATING_COUNT
                                ,GAME_UPDATED_AT
                                ,GAME_URL
                                ,GAME_VERSION_PARENT
                                ,GAME_VERSION_TITLE
                                ,GAME_VIDEOS
                                ,GAME_WEBSITES
                                ,META_JOB_SUMMARY_SK
                                ,META_REQUEST_SK
                            )
                            VALUES
                            (
                                AGE_RATINGS
                                ,AGGREGATED_RATING
                                ,AGGREGATED_RATING_COUNT
                                ,ALTERNATIVE_NAMES
                                ,ARTWORKS
                                ,BUNDLES
                                ,CATEGORY
                                ,COLLECTION
                                ,COVER
                                ,CREATED_AT
                                ,DLCS
                                ,EXPANSIONS
                                ,EXTERNAL_GAMES
                                ,FIRST_RELEASE_DATE
                                ,FOLLOWS
                                ,FRANCHISE
                                ,FRANCHISES
                                ,GAME_ENGINES
                                ,GAME_MODES
                                ,GENRES
                                ,HYPES
                                ,ID
                                ,INVOLVED_COMPANIES
                                ,KEYWORDS
                                ,MULTIPLAYER_MODES
                                ,NAME
                                ,PARENT_GAME
                                ,PLATFORMS
                                ,PLAYER_PERSPECTIVES
                                ,POPULARITY
                                ,PULSE_COUNT
                                ,RATING
                                ,RATING_COUNT
                                ,RELEASE_DATES
                                ,SCREENSHOTS
                                ,SIMILAR_GAMES
                                ,SLUG
                                ,STANDALONE_EXPANSIONS
                                ,STATUS
                                ,STORYLINE
                                ,SUMMARY
                                ,TAGS
                                ,THEMES
                                ,TIME_TO_BEAT
                                ,TOTAL_RATING
                                ,TOTAL_RATING_COUNT
                                ,UPDATED_AT
                                ,URL
                                ,VERSION_PARENT
                                ,VERSION_TITLE
                                ,VIDEOS
                                ,WEBSITES	
                                ,SOURCE.META_JOB_SUMMARY_SK
                                ,{self._dao.job_id}
                            )
        ''')
