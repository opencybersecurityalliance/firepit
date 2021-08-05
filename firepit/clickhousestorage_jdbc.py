import logging
import jaydebeapi
from firepit.clickhouse_common import ClickhouseStorageCommon,ConnectionWrapper

logger = logging.getLogger(__name__)

def get_storage(url, session_id):
    dbname = url.path.lstrip('/')
    return ClickhouseStorageJdbc(dbname, url.geturl(), session_id)

"""
clickhouse storage implementation for Firepit
url pattern:
    clickhousejdbc://<clickhouse_url>:<clickhouse_port>/?user=<clickhouse_user>&password=<clickhouse_password>
"""
class ClickhouseStorageJdbc(ClickhouseStorageCommon):
    def __init__(self, dbname, url, session_id=None):
        super().__init__(dbname,session_id)
        logger.debug("Initializing Clickhouse JDBC Storage")
        url = url.replace("clickhousejdbc://","clickhouse://")
        self.connection = ConnectionWrapper(jaydebeapi.connect(
             "ru.yandex.clickhouse.ClickHouseDriver",
             f"jdbc:{url}",
             {'session_id':f'{session_id}'}
             ))
        self.createDefaultTables()
        logger.debug("Connection to Clickhouse DB %s successful", dbname)
