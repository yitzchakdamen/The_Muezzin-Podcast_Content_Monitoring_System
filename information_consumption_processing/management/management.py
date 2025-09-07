from kafka_tools.kafka_tools import KafkaTools, KafkaConsumer
from data_access_layer.dal_elasticsearch import ElasticSearchDal
from data_access_layer.file_manager import FileManager
from data_access_layer.dal_mongodb import MongoDal
import json, hashlib

class Management:
    
    def __init__(self, 
                dal_elasticsearch: ElasticSearchDal,
                dal_mongo:MongoDal,
                consumer: KafkaConsumer ,
                index_name:str,
                collection_name:str
                ) -> None:
        
        self.consumer = consumer
        self.dal_mongo = dal_mongo
        self.dal_elasticsearch = dal_elasticsearch
        self.index_name = index_name
        self.collection_name = collection_name
        self.dal_elasticsearch.create_index(index_name=self.index_name ,mappings=None)
        
        
    def consumer_loop(self):
        for message in self.consumer:
            self.processing(message)
    
    def processing(self, message):
        has_identifier = self.create_unique_has_identifier(message.value)
        self.insert_metadata_into_elasticsearch(file_id=has_identifier, document=message.value)
        file = FileManager.uploading_content(message.value["absolute_path"])
        self.insert_file_into_mongo(file_id=has_identifier, filename=message.value['name'], document={"content":file})
    
    def create_unique_has_identifier(self, message:dict) -> str:
        message_bytes = json.dumps(message , sort_keys=True).encode('utf-8')
        sha256_hash = hashlib.sha256()
        sha256_hash.update(message_bytes)
        return sha256_hash.hexdigest()
    
    def insert_metadata_into_elasticsearch(self, file_id:str ,document:dict):
        document["file_id"] = file_id
        self.dal_elasticsearch.index_document(document=document, index_name=self.index_name)
    
    def insert_file_into_mongo(self, file_id:str, filename:str, document):
        document["file_id"] = file_id
        self.dal_mongo.insert(collection_name=self.collection_name, documents=document)
        # self.dal_mongo.insert_file(file=file , collection_name=self.collection_name, file_id=id, filename=filename)

