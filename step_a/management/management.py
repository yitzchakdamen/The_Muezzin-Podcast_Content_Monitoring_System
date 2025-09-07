from kafka_tools.kafka_tools import KafkaTools
from step_a.management.metadata_processing import FileMetadataProcessing

class Management:
    
    def __init__(self, file_metadata_processing: FileMetadataProcessing, bootstrap_servers:str) -> None:
        self.file_metadata_processing = file_metadata_processing
        self.poducer = KafkaTools.Producer(bootstrap_servers=bootstrap_servers)
        
    def publish_file_metadata_to_kafka(self, topic:str) -> None:
        for message in self.file_metadata_processing.processing_files_information():
            self.poducer.publish_message(topic=topic, message=message)