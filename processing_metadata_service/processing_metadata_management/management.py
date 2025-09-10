from utils.kafka_tools.kafka_tools import KafkaTools
from processing_metadata_service.processing_metadata_management.metadata_processing import FileMetadataProcessing
import logging
from config.config import LOGGER_NAME
from utils.decorators import log_func


logger = logging.getLogger(LOGGER_NAME)

class Management:
    """
    Initializes the objects required for management and speeds up management 
    poducer: 
        Posting about new file metadata
    file_metadata_processing:
        Class for creating metadata for files
    """
    
    def __init__(self, file_metadata_processing: FileMetadataProcessing, poducer:KafkaTools.Producer) -> None:
        self.file_metadata_processing = file_metadata_processing
        self.poducer =poducer
    
    @log_func
    def publish_file_metadata_to_kafka(self, topic:str) -> None:
        """ getting information about files within a folder in publish into Kafka"""
        for file_information in self.file_metadata_processing.get_files_information_in_folder():
            self.poducer.publish_message(topic=topic, message=file_information)