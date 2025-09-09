from pymongo import MongoClient
from gridfs import GridFS, GridOut
from pymongo.cursor import Cursor
from pymongo.command_cursor import CommandCursor
from pymongo.database import Database
from utils.decorators import safe_execute, log_func
import logging
from config.config import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)

class MongoDal:
    
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            logger.info("DataLoader object created.")
        return cls._instance

    @safe_execute()
    def __init__(self, client_string: str, database: str):
        if MongoDal._initialized: return
        self.client: MongoClient = MongoClient(client_string)
        self.set_database(database)
        MongoDal._initialized = True

    @log_func
    def set_database(self, database: str):
        self.db: Database = self.client[database]

    @safe_execute(return_strategy="error")
    def retrieve(self, collection_name: str, query: dict = {}, exclude_id: bool = True,  to_list: bool = False) -> Cursor|list[dict]:
        projection = {'_id': 0} if exclude_id else None
        cursor = self.db[collection_name].find(query, projection)
        return list(cursor) if to_list else cursor

    @safe_execute(return_strategy="error")
    def insert(self, documents: dict, collection_name: str) -> dict:
        result = self.db[collection_name].insert_one(documents)
        return {"acknowledged": result.acknowledged, "inserted_id": result.inserted_id}
    
    @safe_execute(return_strategy="error")
    def insert_file(self,  collection_name: str,file_id:str, file) -> dict:
        fs = GridFS(database=self.db, collection=collection_name)
        return fs.put(file,  content_type=file, file_id=file_id)
    
    def get_file(self, collection_name: str, id) -> GridOut:
        fs = GridFS(database=self.db, collection=collection_name)
        return fs.get(id)
    
    def find_file(self, collection_name: str, field_name:str, find) -> GridOut | None:
        fs = GridFS(database=self.db, collection=collection_name)
        return fs.find_one({field_name:find})

    @safe_execute(return_strategy="error")
    def insert_many(self, documents: list[dict], collection_name: str) -> dict:
        result = self.db[collection_name].insert_many(documents)
        return {"acknowledged": result.acknowledged, "inserted_ids": result.inserted_ids}

    @safe_execute(return_strategy="error")
    def update(self, query: dict, update: dict, collection_name: str) -> dict:
        result = self.db[collection_name].update_one(filter=query, update=update)
        return {"acknowledged": result.acknowledged, "modified_count": result.modified_count}

    @safe_execute(return_strategy="error")
    def update_many(self, query: dict, update: dict, collection_name: str) -> dict:
        result = self.db[collection_name].update_many(filter=query, update=update)
        return {"acknowledged": result.acknowledged, "modified_count": result.modified_count}

    @safe_execute(return_strategy="error")
    def delete(self, query: dict, collection_name: str) -> dict:
        result = self.db[collection_name].delete_one(query)
        return {"acknowledged": result.acknowledged, "deleted_count": result.deleted_count}

    @safe_execute(return_strategy="error")
    def delete_many(self, query: dict, collection_name: str) -> dict:
        result = self.db[collection_name].delete_many(filter=query)
        return {"acknowledged": result.acknowledged, "deleted_count": result.deleted_count}

    @safe_execute(return_strategy="error")
    def retrieve_aggregate(self, collection_name: str, pipeline: list[dict], to_list: bool = False) -> list[dict] | CommandCursor:
        cursor = self.db[collection_name].aggregate(pipeline)
        return list(cursor) if to_list else cursor
