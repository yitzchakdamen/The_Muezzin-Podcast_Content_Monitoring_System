from kafka_tools.kafka_tools import KafkaTools
from processing_metadata.management.metadata_processing import FileMetadataProcessing

class Management:
    
    def __init__(self, file_metadata_processing: FileMetadataProcessing, poducer:KafkaTools.Producer) -> None:
        self.file_metadata_processing = file_metadata_processing
        self.poducer =poducer
        
    def publish_file_metadata_to_kafka(self, topic:str) -> None:
        for message in self.file_metadata_processing.processing_files_information():
            self.poducer.publish_message(topic=topic, message=message)