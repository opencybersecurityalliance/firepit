import logging
from urllib import parse
from clickhouse_driver import connect
from firepit.clickhouse_common import ClickhouseStorageCommon,ConnectionWrapper


logger = logging.getLogger(__name__)

def get_storage(url, session_id):
    dbname = url.path.lstrip('/')
    return ClickhouseStorageNative(dbname, url.geturl(), session_id)

"""
clickhouse storage implementation for Firepit
url pattern:
    clickhouse://<clickhouse_url>:<clickhouse_port>/?user=<clickhouse_user>&password=<clickhouse_password>
"""
class ClickhouseStorageNative(ClickhouseStorageCommon):
    def __init__(self, dbname, url, session_id=None):
        super().__init__(dbname,session_id)
        logger.debug("Initializing Clickhouse Native Storage")
        urlObj=parse.urlparse(url)
        host= urlObj.netloc.split(":")[0]
        port= urlObj.netloc.split(":")[1]
        query_params  = parse.parse_qs(urlObj.query)
        self.connection = ConnectionWrapper(connect(
                                                host=host,
                                                port=port,
                                                user=query_params['user'][0],
                                                password=query_params['password'][0],
                                                database='default'
                                                )
                                            )
        self.createDefaultTables()
        logger.debug("Connection to Clickhouse DB %s successful", dbname)
