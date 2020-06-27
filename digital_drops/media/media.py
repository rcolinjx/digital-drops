from abc import abstractmethod

from digital_drops import util
from digital_drops.constants import SQL
from digital_drops.dao import SnowflakeDao


class MediaProvider(object):
    def __init__(self, dao: SnowflakeDao, media_provider: str, media_type: str):
        self._media_provider = media_provider
        self._media_type = media_type

        self._dao = dao
        self._target_table = self._dao.get_target_table(media_type)
        self._api_key = util.get_connection_keys(f'''key_api_{self._media_type}_{self._media_provider}'''.lower())

    def create_insert_query(self, request_url: str, response_text: str, response_page: str):
        return SQL.INSERT.format(table_name=self._dao.staging_table, request_provider=self._media_provider,
                                 request_endpoint=request_url, request_response=response_text,
                                 response_page=response_page, job_id=self._dao.job_id)

    def extract_load(self):
        print(f'''Starting extract/load from {self._media_provider}''')
        self._extract_from_api()
        self._dao.update_recent_requests(self._media_provider)
        print(f'''Finished extract/load from {self._media_provider}''')

    @abstractmethod
    def _extract_from_api(self):
        pass

    @abstractmethod
    def _load_staging(self, *args, **kwargs):
        pass

    @abstractmethod
    def _transform(self):
        pass

    def transform(self):
        print(f'''Starting transform of {self._media_provider}''')
        self._transform()
        print(f'''Completed transform of {self._media_provider}''')
