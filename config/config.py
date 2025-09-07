import os
import logging


logger = logging.getLogger(__name__)




PODCAST_FILES_PATH = os.getenv("PODCAST_FILES_PATH", r"data\podcasts")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_FILE_METADATA = os.getenv("KAFKA_TOPIC_FILE_METADATA", "file-metadata")





logger.debug(f"""
            Configuration Loaded ___ :
            PODCAST_FILES_PATH:( {PODCAST_FILES_PATH})
            BOOTSTRAP_SERVERS: ({BOOTSTRAP_SERVERS})
            KAFKA_TOPIC_FILE_METADATA: ({KAFKA_TOPIC_FILE_METADATA})
            """)