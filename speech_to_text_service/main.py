from config import config
from config.logger_config import LoggerConfig
import logging
from speech_to_text_service.stt_management.management import Management
from utils.kafka_tools.kafka_tools import KafkaTools
from utils.data_access_layer.dal_mongodb import MongoDal
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal
from config.config import LOGGER_NAME

logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)
logger = logging.getLogger(LOGGER_NAME)

BOOTSTRAP_SERVERS = config.BOOTSTRAP_SERVERS
KAFKA_TOPIC_INCOME_PROCESSING = config.KAFKA_TOPIC_INCOME_PROCESSING
KAFKA_GROUP_ID_INCOME_PROCESSING = config.KAFKA_GROUP_ID_INCOME_PROCESSING

ELASTICSEARCH_HOST = config.ELASTICSEARCH_HOST
ELASTICSEARCH_INDEX_TRANSCRIPTTION = config.ELASTICSEARCH_INDEX_TRANSCRIPTTION
ELASTICSEARCH_MAPPING = config.ELASTICSEARCH_MAPPING
ELASTICSEARCH_INDEX_LOG = config.ELASTICSEARCH_INDEX_LOG

MONGO_CLIENT_STRING = config.MONGO_CLIENT_STRING
MONGO_DB = config.MONGO_DB
MONGO_COLLECTION = config.MONGO_COLLECTION

def main():
    
    logging.basicConfig(level=logging.INFO, handlers=LoggerConfig.config_ESHandler(es_host=ELASTICSEARCH_HOST, index=ELASTICSEARCH_INDEX_LOG))
    
    logger.info(" ____ Starting the application ____ ")
    
    consumer = KafkaTools.Consumer.get_consumer(
        KAFKA_TOPIC_INCOME_PROCESSING, 
        bootstrap_servers=BOOTSTRAP_SERVERS, 
        group_id=KAFKA_GROUP_ID_INCOME_PROCESSING)
    
    dal_elasticsearch = ElasticSearchDal(elasticsearch_host=ELASTICSEARCH_HOST)
    dal_mongo = MongoDal(client_string=MONGO_CLIENT_STRING, database=MONGO_DB)
    
    management = Management(
        dal_elasticsearch=dal_elasticsearch,
        dal_mongo= dal_mongo, 
        consumer=consumer ,
        index_name=ELASTICSEARCH_INDEX_TRANSCRIPTTION,
        collection_name=MONGO_COLLECTION,
        )
    management.consumer_loop()

if __name__ == "__main__":
    # python -m speech_to_text_service.main
    main()