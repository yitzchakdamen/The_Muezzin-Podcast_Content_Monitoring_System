import logging
from config import config
from utils import decorators
from kafka_tools.kafka_tools import KafkaTools
from utils.decorators import log
from step_a.management.metadata_processing import FileMetadataProcessing



logging.basicConfig(level=logging.DEBUG)
logging.getLogger('kafka').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

PODCAST_FILES_PATH = config.PODCAST_FILES_PATH

def main():
    logger.info(" ____ Starting the application ____ ")
    file_metadata_processing = FileMetadataProcessing(PODCAST_FILES_PATH)
    metadata = file_metadata_processing.processing_files_information()
    print(metadata)


if __name__ == "__main__":
    # python -m step_a.main
    main()

