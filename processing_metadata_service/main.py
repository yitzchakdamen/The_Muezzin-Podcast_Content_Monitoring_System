import logging
from config import config
from utils.kafka_tools.kafka_tools import KafkaTools
from processing_metadata_service.processing_metadata_management.metadata_processing import FileMetadataProcessing
from processing_metadata_service.processing_metadata_management.management import Management


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('kafka').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

PODCAST_FILES_PATH = config.PODCAST_FILES_PATH
BOOTSTRAP_SERVERS = config.BOOTSTRAP_SERVERS
KAFKA_TOPIC_FILE_METADATA = config.KAFKA_TOPIC_FILE_METADATA


def main():
    logger.info(" ____ Starting the application ____ ")
    
    file_metadata_processing = FileMetadataProcessing(PODCAST_FILES_PATH)
    poducer = KafkaTools.Producer(bootstrap_servers=BOOTSTRAP_SERVERS)
    
    management = Management(file_metadata_processing=file_metadata_processing, poducer=poducer)
    management.publish_file_metadata_to_kafka(topic=KAFKA_TOPIC_FILE_METADATA)

if __name__ == "__main__":
    # python -m processing_metadata_service.main
    main()
