import os
import logging


logger = logging.getLogger(__name__)




PODCAST_FILES_PATH = os.getenv("PODCAST_FILES_PATH", r"data\podcasts")


ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")





logger.info(f"""
            Configuration Loaded ___ :
            ELASTICSEARCH_HOST: {ELASTICSEARCH_HOST}
            """)