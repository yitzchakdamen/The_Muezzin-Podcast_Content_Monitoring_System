import logging
from config import config
from storage_service.storage_service_management.management import Management
from utils.kafka_tools.kafka_tools import KafkaTools
from utils.data_access_layer.dal_mongodb import MongoDal
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal


logging.basicConfig(level=logging.INFO)
logging.getLogger('kafka').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


BOOTSTRAP_SERVERS = config.BOOTSTRAP_SERVERS
KAFKA_TOPIC_FILE_METADATA = config.KAFKA_TOPIC_FILE_METADATA
KAFKA_GROUP_ID_FILE_METADATA = config.KAFKA_GROUP_ID_FILE_METADATA

ELASTICSEARCH_HOST = config.ELASTICSEARCH_HOST
ELASTICSEARCH_INDEX = config.ELASTICSEARCH_INDEX
ELASTICSEARCH_MAPPING = config.ELASTICSEARCH_MAPPING

MONGO_CLIENT_STRING = config.MONGO_CLIENT_STRING
MONGO_DB = config.MONGO_DB
MONGO_COLLECTION = config.MONGO_COLLECTION




def main():
    logger.info(" ____ Starting the application ____ ")
    
    consumer = KafkaTools.Consumer.get_consumer(
        KAFKA_TOPIC_FILE_METADATA, 
        bootstrap_servers=BOOTSTRAP_SERVERS, 
        group_id=KAFKA_GROUP_ID_FILE_METADATA)
    
    dal_elasticsearch = ElasticSearchDal(elasticsearch_host=ELASTICSEARCH_HOST)
    dal_mongo = MongoDal(client_string=MONGO_CLIENT_STRING, database=MONGO_DB)
    
    management = Management(
        dal_elasticsearch=dal_elasticsearch,
        dal_mongo= dal_mongo, 
        consumer=consumer ,
        index_name=ELASTICSEARCH_INDEX,
        collection_name=MONGO_COLLECTION,
        elasticsearch_mapping=ELASTICSEARCH_MAPPING
        )
    management.consumer_loop()

if __name__ == "__main__":
    # python -m storage_service.main
    main()
