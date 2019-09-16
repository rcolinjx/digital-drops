import common
import requests


class Spotify:
    name = 'Spotify'
    type = 'SONG'
    conn = None
    api_key = None
    table_stage = None
    table_model = None

    def extract_load_upcoming(self):
        try:
            path = 'search'
            url = f'''ttps://api.spotify.com/v1/{path}?q='''
            page_num = 1

            res_json = self.get_response(url, page_num)

            page_max = int(res_json['total_pages'])

            if page_max > 1:

                while page_num < page_max:
                    page_num += 1
                    self.get_response(url, page_num)

            self.conn.update_recent_requests(self.table_stage, self.name)

        except Exception as ex:
            print('Exception: ' + str(ex))

    def get_response(self, url, page):
        return None

    def transform_upcoming(self):
       return None

    def __init__(self, conn):
        self.api_key = common.get_connection_keys(f'''key_api_{self.type}_{self.name}'''.lower())
        self.conn = conn
        self.table_stage = conn.tbl_stg
        self.table_model = '.'.join([conn.db, self.type])
