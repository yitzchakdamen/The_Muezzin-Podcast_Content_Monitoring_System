import os
import logging
import json


logger = logging.getLogger(__name__)




PODCAST_FILES_PATH = os.getenv("PODCAST_FILES_PATH", r"data\podcasts")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_FILE_METADATA = os.getenv("KAFKA_TOPIC_FILE_METADATA", "file-metadata")
KAFKA_GROUP_ID_FILE_METADATA = os.getenv("KAFKA_GROUP_ID_FILE_METADATA", "information_consumption_processing")

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "audio_files-metadata")
ELASTICSEARCH_MAPPING_PATH = os.getenv("ELASTICSEARCH_MAPPING_PATH", r"config\elasticsearch_mapping.json")
with open(ELASTICSEARCH_MAPPING_PATH) as f: ELASTICSEARCH_MAPPING = json.load(f)

MONGO_CLIENT_STRING = os.getenv("MONGO_CLIENT_STRING", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "podcast_files")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "audio_files")





logger.debug(f"""
            Configuration Loaded ___ :
            PODCAST_FILES_PATH:( {PODCAST_FILES_PATH})
            BOOTSTRAP_SERVERS: ({BOOTSTRAP_SERVERS})
            KAFKA_TOPIC_FILE_METADATA: ({KAFKA_TOPIC_FILE_METADATA})
            KAFKA_GROUP_ID_FILE_METADATA: ({KAFKA_GROUP_ID_FILE_METADATA})
            """)