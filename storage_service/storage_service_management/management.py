from utils.kafka_tools.kafka_tools import KafkaTools, KafkaConsumer
from utils.data_access_layer.dal_elasticsearch import ElasticSearchDal
from utils.data_access_layer.file_manager import FileManager
from utils.data_access_layer.dal_mongodb import MongoDal
import json, hashlib, logging
from utils.decorators import log_func, safe_execute

logger = logging.getLogger(__name__)

class Management:
    
    def __init__(self, 
                dal_elasticsearch: ElasticSearchDal,
                dal_mongo:MongoDal,
                consumer: KafkaConsumer ,
                index_name:str,
                collection_name:str,
                elasticsearch_mapping:dict
                ) -> None:
        """ """
        self.consumer = consumer
        self.dal_mongo = dal_mongo
        self.dal_elasticsearch = dal_elasticsearch
        self.index_name = index_name
        self.collection_name = collection_name
        self.dal_elasticsearch.create_index(index_name=self.index_name ,mappings=elasticsearch_mapping)
        
    @log_func 
    def consumer_loop(self) -> None:
        """ Listening on Kafka and running processing on each message"""
        for message in self.consumer:
            logger.debug("message from Kafka: {message}")
            self.processing(message.value)
    
    @safe_execute(return_strategy="None")
    def path_processing(self, message):
        message.pop("relative_path", None)
        return message.pop("absolute_path", None)
    
    @log_func
    def processing(self, message) -> None:
        """ 
        Processing on message:
            Create a unique ID
            Insert metadata and file into databases - Mongo and Elastic
        """
        has_identifier = self.create_unique_hash_identifier(message)
        path = self.path_processing(message)
        file = FileManager.uploading_content(path)
        
        self.index_metadata_into_elasticsearch(file_id=has_identifier, data=message)
        self.insert_file_into_mongo(file_id=has_identifier, document={"content":file})
    
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
        self.dal_elasticsearch.index_document(document=data, index_name=self.index_name)
    
    @log_func
    def insert_file_into_mongo(self, file_id:str, document):
        """Attach the id and insert the file - binary into elasticsearch """
        document["file_id"] = file_id
        self.dal_mongo.insert(collection_name=self.collection_name, documents=document)
