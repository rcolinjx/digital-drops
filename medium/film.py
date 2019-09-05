import requests


class TMDB:
    api_key = ''
    table = ''
    job_id = ''

    def extract_load_upcoming(self, cur):
        try:
            path = '/movie/upcoming'
            params = ['language=EN', 'region=US']
            url = 'https://api.themoviedb.org/3' + path + '?' + '&'.join(params)
            page_cur = 1

            res_json = self.get_response(url, page_cur, cur)

            page_max = int(res_json['total_pages'])

            if page_max > 1:
                while page_cur <= page_max:
                    page_cur += 1
                    self.get_response(url, page_cur, cur)

        except Exception as ex:
            print('Exception: ' + str(ex))

    def get_response(self, url, page, cur):
        req_sec = url + '&page=' + str(page) + '&api_key='

        req = requests.get(req_sec + self.api_key)

        res_json = req.json()

        if not res_json['results']:
            raise Exception(res_json['status_message'])

        sql = '''INSERT INTO {0}(REQUEST_ENDPOINT, REQUEST_RESPONSE, META_PAGE_NUMBER, META_JOB_SUMMARY_SK)
              SELECT '{1}', PARSE_JSON($${2}$$), {3}, {4};'''.format(self.table, req_sec, req.text, page, self.job_id)

        cur.execute(sql)

        return res_json

    def __init__(self, api_key, table, job_id):
        self.api_key = api_key
        self.table = table
        self.job_id = job_id
