from fastapi import FastAPI
from config import config
from config.logger_config import LoggerConfig
import logging, uvicorn
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal
from config.config import LOGGER_NAME

APP_HOST = config.APP_HOST
APP_PORT = config.APP_PORT

ELASTICSEARCH_HOST = config.ELASTICSEARCH_HOST
ELASTICSEARCH_INDEX_TRANSCRIPTTION = config.ELASTICSEARCH_INDEX_TRANSCRIPTTION
ELASTICSEARCH_INDEX_LOG = config.ELASTICSEARCH_INDEX_LOG

logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)
logger = logging.getLogger(LOGGER_NAME)
logging.basicConfig(level=logging.INFO, handlers=LoggerConfig.config_ESHandler(es_host=ELASTICSEARCH_HOST, index=ELASTICSEARCH_INDEX_LOG))
    
dal_elasticsearch = ElasticSearchDal(elasticsearch_host=ELASTICSEARCH_HOST)
app = FastAPI()


@app.get("/api/documents-danger-level/{level_gte}/{level_lte}")
async def get_antisemitic_documents(level_gte: float, level_lte: float):
    query = {"range": {"bds_percent": {"gte":level_gte, "lte": level_lte}}}
    search = dal_elasticsearch.search_document(index_name=ELASTICSEARCH_INDEX_TRANSCRIPTTION, query=query)
    return [document['_source'] for document in search['hits']['hits']]


if __name__ == "__main__":
    # python -m api_endpoint_service.main
    uvicorn.run(app, host=config.APP_HOST, port=config.APP_PORT)
    
    
    
