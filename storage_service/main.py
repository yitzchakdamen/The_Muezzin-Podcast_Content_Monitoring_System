from config import config
from config.logger_config import LoggerConfig
import logging
from storage_service.storage_management.management import Management
from utils.kafka_tools.kafka_tools import KafkaTools
from utils.data_access_layer.dal_mongodb import MongoDal
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal
from config.config import LOGGER_NAME

logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)
logger = logging.getLogger(LOGGER_NAME)

BOOTSTRAP_SERVERS = config.BOOTSTRAP_SERVERS
KAFKA_TOPIC_FILE_METADATA = config.KAFKA_TOPIC_FILE_METADATA
KAFKA_GROUP_ID_FILE_METADATA = config.KAFKA_GROUP_ID_FILE_METADATA

KAFKA_TOPIC_INCOME_PROCESSING = config.KAFKA_TOPIC_INCOME_PROCESSING

ELASTICSEARCH_HOST = config.ELASTICSEARCH_HOST
ELASTICSEARCH_INDEX = config.ELASTICSEARCH_INDEX
ELASTICSEARCH_MAPPING = config.ELASTICSEARCH_MAPPING
ELASTICSEARCH_INDEX_LOG = config.ELASTICSEARCH_INDEX_LOG

MONGO_CLIENT_STRING = config.MONGO_CLIENT_STRING
MONGO_DB = config.MONGO_DB
MONGO_COLLECTION = config.MONGO_COLLECTION

def main():
    """
    Initializes the objects required for management and speeds up management 
    consumer: 
        Getting file metadata information
    poducer: 
        Posting a message about storing a file in Mongo
    dal_elasticsearch:
        for connecting to Elasticsearch
    dal_mongo:
        for connecting to Mongo
    """
    logging.basicConfig(level=logging.INFO, handlers=LoggerConfig.config_ESHandler(es_host=ELASTICSEARCH_HOST, index=ELASTICSEARCH_INDEX_LOG))
    
    logger.info(" ____ Starting the application ____ ")
    
    consumer = KafkaTools.Consumer.get_consumer(
        KAFKA_TOPIC_FILE_METADATA, 
        bootstrap_servers=BOOTSTRAP_SERVERS, 
        group_id=KAFKA_GROUP_ID_FILE_METADATA)
    
    poducer = KafkaTools.Producer(bootstrap_servers=BOOTSTRAP_SERVERS)
    
    dal_elasticsearch = ElasticSearchDal(elasticsearch_host=ELASTICSEARCH_HOST)
    dal_mongo = MongoDal(client_string=MONGO_CLIENT_STRING, database=MONGO_DB)
    
    management = Management(
        dal_elasticsearch=dal_elasticsearch,
        dal_mongo= dal_mongo, 
        consumer=consumer ,
        producer= poducer,
        index_name=ELASTICSEARCH_INDEX,
        collection_name=MONGO_COLLECTION,
        elasticsearch_mapping=ELASTICSEARCH_MAPPING
        )
    management.consumer_loop(topic=KAFKA_TOPIC_INCOME_PROCESSING)

if __name__ == "__main__":
    # python -m storage_service.main
    main()