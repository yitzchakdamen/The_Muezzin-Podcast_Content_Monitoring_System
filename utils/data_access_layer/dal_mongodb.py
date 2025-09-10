from pymongo import MongoClient
from gridfs import GridFS, GridOut
from pymongo.database import Database
from utils.decorators import safe_execute, log_func
import logging
from config.config import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)

class MongoDal:
    
    """
    Singleton class To verify a single connection with Mongo
    data access layer for Mongo
    """
    
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            logger.info("DataLoader object created.")
        return cls._instance


    def __init__(self, client_string: str, database: str):
        if MongoDal._initialized: return
        self.client: MongoClient = MongoClient(client_string)
        self.set_database(database)
        MongoDal._initialized = True

    @log_func
    def set_database(self, database: str):
        self.db: Database = self.client[database]

    @safe_execute(return_strategy="error")
    def insert(self, documents: dict, collection_name: str) -> dict:
        result = self.db[collection_name].insert_one(documents)
        return {"acknowledged": result.acknowledged, "inserted_id": result.inserted_id}
    
    @safe_execute(return_strategy="error")
    def insert_file(self,  collection_name: str,file_id:str, file) -> dict:
        """Using GridFS for efficient storage in Mongo"""
        fs = GridFS(database=self.db, collection=collection_name)
        return fs.put(file,  content_type=file, file_id=file_id)
    
    def get_file(self, collection_name: str, id) -> GridOut:
        """ For files we stored via GridFS """
        fs = GridFS(database=self.db, collection=collection_name)
        return fs.get(id)
    
    def find_file(self, collection_name: str, field_name:str, find) -> GridOut | None:
        """ For files we stored via GridFS """
        fs = GridFS(database=self.db, collection=collection_name)
        return fs.find_one({field_name:find})

