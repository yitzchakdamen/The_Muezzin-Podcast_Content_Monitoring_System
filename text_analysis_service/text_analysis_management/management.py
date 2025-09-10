from utils.kafka_tools.kafka_tools import KafkaTools, KafkaConsumer
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal
from text_analysis_service.text_analysis_management.text_analysis import TextAnalysis
import logging
from utils.decorators import log_func, safe_execute
from config.config import LOGGER_NAME
from typing import BinaryIO
from faster_whisper import WhisperModel


logger = logging.getLogger(LOGGER_NAME)

class Management:
    """
    Service Management:
    Contains the consumer loop function. 
    Processing each message. 
    And retrieving from Mango
    """
    
    def __init__(self, 
                text_analysis: TextAnalysis, 
                dal_elasticsearch: ElasticSearchDal,
                consumer: KafkaConsumer ,
                index_name:str,
                ) -> None:
        """ Initializing the class with the required fields - the DAL for the databases - Kafka Consumer"""
        self.consumer = consumer
        self.dal_elasticsearch = dal_elasticsearch
        self.index_name = index_name
        self.text_analysis = text_analysis

        
    @log_func 
    def consumer_loop(self) -> None:
        """ Listening on Kafka and running processing on each message"""
        for message in self.consumer:
            logger.info(f"message from Kafka: {message}")
            self.message_reception_processing(message.value)
            
    @safe_execute()
    def message_reception_processing(self, message:dict):
        """
        Processing process for each message 
        - extracts the ID 
        - extracts the text 
        - parses 
        - updates the document in Elastic
        """
        file_id:str = message["file_id"]
        text, id = self.get_file_from_elasticsearch(file_id=file_id) 
        analysis = self.text_analysis.snalysis_process(text)
        self.dal_elasticsearch.update_document(index_name=self.index_name, document=analysis, id=id)
        # logging.info(f"file with id: {file_id}  and indexed into elasticsearch")
        

    @safe_execute()
    def get_file_from_elasticsearch(self, file_id:str) -> tuple:
        """Extracts the ID and text of the transcript from an Elasticsearch document via file_id"""
        file = self.dal_elasticsearch.search_document(index_name=self.index_name, query= {"match": {"file_id":file_id}})
        text, id = file['hits']['hits'][0]["_source"]["transcribe"],  file['hits']['hits'][0]['_id']
        
        logger.info(f"get for file_id {file_id} text: {text[:20]} id: {id}")
        return text, id 
    
    
    

    
