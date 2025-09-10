from config import config
from config.logger_config import LoggerConfig
import logging
from text_analysis_service.text_analysis_management.management import Management
from text_analysis_service.text_analysis_management.text_analysis import TextAnalysis
from utils.kafka_tools.kafka_tools import KafkaTools
from utils.data_access_layer.dal_mongodb import MongoDal
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal
from config.config import LOGGER_NAME

logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)
logger = logging.getLogger(LOGGER_NAME)

BOOTSTRAP_SERVERS = config.BOOTSTRAP_SERVERS

KAFKA_TOPIC_TRANSCRIPTTION = config.KAFKA_TOPIC_TRANSCRIPTTION
KAFKA_GROUP_ID_TRANSCRIPTTION = config.KAFKA_GROUP_ID_TRANSCRIPTTION

ELASTICSEARCH_HOST = config.ELASTICSEARCH_HOST
ELASTICSEARCH_INDEX_TRANSCRIPTTION = config.ELASTICSEARCH_INDEX_TRANSCRIPTTION
ELASTICSEARCH_MAPPING = config.ELASTICSEARCH_MAPPING
ELASTICSEARCH_INDEX_LOG = config.ELASTICSEARCH_INDEX_LOG


HOSTILE_TEXT = config.HOSTILE_TEXT
LESS_HOSTILE_TEXT = config.LESS_HOSTILE_TEXT
HIGH_DANGER_LEVEL = config.HIGH_DANGER_LEVEL
MEDIUM_DANGER_LEVEL = config.MEDIUM_DANGER_LEVEL
THRESHOLD = config.THRESHOLD

def main():
    """
    Initializes the objects required for management and speeds up management 
    consumer: 
        Get information about a transcript uploaded to Elasticsearch
    dal_elasticsearch:
        for connecting to Elasticsearch
    text analysis:
        to process text for ranking
    """
    
    logging.basicConfig(level=logging.INFO, handlers=LoggerConfig.config_ESHandler(es_host=ELASTICSEARCH_HOST, index=ELASTICSEARCH_INDEX_LOG))
    
    logger.info(" ____ Starting the application ____ ")
    
    consumer = KafkaTools.Consumer.get_consumer(
        KAFKA_TOPIC_TRANSCRIPTTION, 
        bootstrap_servers=BOOTSTRAP_SERVERS, 
        group_id=KAFKA_GROUP_ID_TRANSCRIPTTION)
    
    dal_elasticsearch = ElasticSearchDal(elasticsearch_host=ELASTICSEARCH_HOST)

    text_analysis = TextAnalysis(
        hostile_text=HOSTILE_TEXT, 
        less_hostile_text=LESS_HOSTILE_TEXT,
        high_danger_level=HIGH_DANGER_LEVEL,
        medium_danger_level=MEDIUM_DANGER_LEVEL,
        threshold=THRESHOLD
        )
    
    management = Management(
        text_analysis = text_analysis,
        dal_elasticsearch=dal_elasticsearch,
        consumer=consumer ,
        index_name=ELASTICSEARCH_INDEX_TRANSCRIPTTION,
        )
    management.consumer_loop()
    
    

if __name__ == "__main__":
    # python -m speech_to_text_service.main
    main()
    
    
    



