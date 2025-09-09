from utils.kafka_tools.kafka_tools import KafkaTools, KafkaConsumer
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal
from utils.data_access_layer.file_manager import FileManager
from utils.data_access_layer.dal_mongodb import MongoDal
import json, hashlib, logging, io
from utils.decorators import log_func, safe_execute
from config.config import LOGGER_NAME
from typing import BinaryIO
from faster_whisper import WhisperModel


logger = logging.getLogger(LOGGER_NAME)

class Management:
    
    def __init__(self, 
                dal_elasticsearch: ElasticSearchDal,
                dal_mongo:MongoDal,
                consumer: KafkaConsumer ,
                index_name:str,
                collection_name:str,
                ) -> None:
        """ Initializing the class with the required fields - the DAL for the databases - Kafka Consumer"""
        self.consumer = consumer
        self.dal_mongo = dal_mongo
        self.dal_elasticsearch = dal_elasticsearch
        self.index_name = index_name
        self.collection_name = collection_name
        self.dal_elasticsearch.create_index(index_name=self.index_name ,mappings=None)
        self.model = WhisperModel("base", device="cpu")
        
    @log_func 
    def consumer_loop(self) -> None:
        """ Listening on Kafka and running processing on each message"""
        for message in self.consumer:
            logger.info(f"message from Kafka: {message}")
            self.message_reception_processing(message.value)
    
    @safe_execute()
    def message_reception_processing(self, message):
        file_id = message["file_id"]
        bytes_file = self.retrieve_from_mongo(file_id)
        transcribe = self.transcription(io.BytesIO(bytes_file))
        self.dal_elasticsearch.index_document(index_name=self.index_name, document= {"file_id":file_id, "transcribe":transcribe})
        logging.info(f"file with id: {file_id} transcribed and indexed into elasticsearch")

    def retrieve_from_mongo(self, file_id) -> bytes:
        logger.info(f"retrieve file from mongo {file_id}")
        
        find = self.dal_mongo.find_file(collection_name=self.collection_name, field_name="file_id", find=file_id)
        if find: return self.dal_mongo.get_file(collection_name=self.collection_name, id=find._id).read()
        else: raise ValueError("file not find")
        
    def transcription(self, audio_stream: BinaryIO) -> str:
        segments, info = self.model.transcribe(audio_stream, language='en')
        return "".join([segment.text for segment in segments ])

    
    