from utils.kafka_tools.kafka_tools import KafkaTools, KafkaConsumer, KafkaProducer
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal
from utils.data_access_layer.file_manager import FileManager
from utils.data_access_layer.dal_mongodb import MongoDal
import json, hashlib, logging
from utils.decorators import log_func, safe_execute
from config.config import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)

class Management:
    
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
            logger.debug(f"message from Kafka: {message}")
            self.processing(message.value, topic)
            
            
    
    @safe_execute(return_strategy="None")
    def path_processing(self, message):
        message.pop("relative_path", None)
        return message.pop("absolute_path", None)
    
    @log_func
    def processing(self, message, topic:str) -> None:
        """ 
        Processing on message:
            Create a unique ID
            Insert metadata and file into databases - Mongo and Elastic
        """
        has_identifier = self.create_unique_hash_identifier(message)
        path = self.path_processing(message)
        file = FileManager.uploading_content(path)
        
        index_metadata = self.index_metadata_into_elasticsearch(file_id=has_identifier, data=message)
        insert_file_into_mongo = self.insert_file_into_mongo(file_id=has_identifier, file=file)

        self.producer.publish_message(message={"file_id":has_identifier, "info":"inserted to mongo"},topic=topic)

    @log_func
    def create_unique_hash_identifier(self, message:dict) -> str:
        """ Creating a unique identifier using hash"""
        message_bytes = json.dumps(message , sort_keys=True).encode('utf-8')
        sha256_hash = hashlib.sha256()
        sha256_hash.update(message_bytes)
        return sha256_hash.hexdigest()
    
    @log_func
    def index_metadata_into_elasticsearch(self, file_id:str ,data:dict):
        """Attach the id and insert the metadata into elasticsearch """
        data["file_id"] = file_id
        return self.dal_elasticsearch.index_document(document=data, index_name=self.index_name)
    
    @log_func
    def insert_file_into_mongo(self, file_id:str, file):
        """Attach the id and insert the file - binary into elasticsearch """
        return self.dal_mongo.insert_file(collection_name=self.collection_name, file_id=file_id, file=file)
