import logging
from config import config
from utils.kafka_tools.kafka_tools import KafkaTools
from processing_metadata_service.processing_metadata_management.metadata_processing import FileMetadataProcessing
from processing_metadata_service.processing_metadata_management.management import Management
from config.logger_config import LoggerConfig
from config.config import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)

PODCAST_FILES_PATH = config.PODCAST_FILES_PATH
BOOTSTRAP_SERVERS = config.BOOTSTRAP_SERVERS
KAFKA_TOPIC_FILE_METADATA = config.KAFKA_TOPIC_FILE_METADATA
ELASTICSEARCH_HOST = config.ELASTICSEARCH_HOST
ELASTICSEARCH_INDEX_LOG = config.ELASTICSEARCH_INDEX_LOG


def main():
    logger.info(" ____ Starting the application ____ ")
    logging.basicConfig(level=logging.INFO, handlers=LoggerConfig.config_ESHandler(es_host=ELASTICSEARCH_HOST, index=ELASTICSEARCH_INDEX_LOG))
    logging.getLogger('kafka').setLevel(logging.WARNING)
    
    file_metadata_processing = FileMetadataProcessing(PODCAST_FILES_PATH)
    poducer = KafkaTools.Producer(bootstrap_servers=BOOTSTRAP_SERVERS)
    
    management = Management(file_metadata_processing=file_metadata_processing, poducer=poducer)
    management.publish_file_metadata_to_kafka(topic=KAFKA_TOPIC_FILE_METADATA)

if __name__ == "__main__":
    # python -m processing_metadata_service.main
    main()
