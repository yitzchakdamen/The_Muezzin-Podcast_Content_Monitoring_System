from elastic_transport import ObjectApiResponse
from utils.kafka_tools.kafka_tools import KafkaTools, KafkaConsumer
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal
from utils.data_access_layer.file_manager import FileManager
from utils.data_access_layer.dal_mongodb import MongoDal
import json, hashlib, logging
from utils.decorators import log_func, safe_execute
from config.config import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)

class Management:
    
    """
    Service Management:
    Contains the consumer loop function. 
    Processing each message. 
    Database input functions.
    And Creating a unique ID
    """
    
    def __init__(self, 
                dal_elasticsearch: ElasticSearchDal,
                dal_mongo:MongoDal,
                consumer: KafkaConsumer ,
                producer: KafkaTools.Producer,
                index_name:str,
                collection_name:str,
                elasticsearch_mapping:dict
                ) -> None:
        """ Initializing the class with the required fields - the DAL for the databases - Kafka Consumer"""
        self.consumer = consumer
        self.producer = producer
        self.dal_mongo = dal_mongo
        self.dal_elasticsearch = dal_elasticsearch
        self.index_name = index_name
        self.collection_name = collection_name
        self.dal_elasticsearch.create_index(index_name=self.index_name ,mappings=elasticsearch_mapping)
        
    @log_func 
    def consumer_loop(self, topic:str) -> None:
        """ Listening on Kafka and running processing on each message"""
        for message in self.consumer:
            logger.info(f"message from Kafka: {message}")
            self.processing(message.value, topic)
    
    @log_func
    def processing(self, message, topic:str) -> None:
        """ 
        Processing on message:
            Create a unique ID
            Insert metadata and file into databases - Mongo and Elastic
            publish in Kafka
        """
        has_identifier = self.create_unique_hash_identifier(message)
        path: str = self.path_processing(message)
        file: bytes = FileManager.uploading_content(path) # type: ignore
        
        index_metadata = self.index_metadata_into_elasticsearch(file_id=has_identifier, data=message)
        insert_file_into_mongo = self.insert_file_into_mongo(file_id=has_identifier, file=file)
        
        if insert_file_into_mongo: self.producer.publish_message(message={"file_id":has_identifier, "info":"file inserted into mongo"},topic=topic)

    @log_func
    def create_unique_hash_identifier(self, message:dict) -> str:
        """ Creating a unique identifier using hash"""
        message_bytes = json.dumps(message , sort_keys=True).encode('utf-8')
        sha256_hash = hashlib.sha256()
        sha256_hash.update(message_bytes)
        return sha256_hash.hexdigest()
    
    @safe_execute(return_strategy="None")
    def path_processing(self, message: dict, absolute_path: bool=False) -> str:
        """ Extract the path to the file from a message - Relative or absolute """
        if not absolute_path:
            message.pop("relative_path", None)
            return message.pop("absolute_path", None)
        else:
            message.pop("absolute_path", None)
            return message.pop("relative_path", None)
            
    @log_func
    def index_metadata_into_elasticsearch(self, file_id:str ,data:dict) -> ObjectApiResponse:
        """Attach the id and insert the metadata into elasticsearch """
        data["file_id"] = file_id
        return self.dal_elasticsearch.index_document(document=data, index_name=self.index_name)
    
    @log_func
    def insert_file_into_mongo(self, file_id:str, file) -> dict:
        """Attach the id and insert the file - binary into elasticsearch """
        return self.dal_mongo.insert_file(collection_name=self.collection_name, file_id=file_id, file=file)
