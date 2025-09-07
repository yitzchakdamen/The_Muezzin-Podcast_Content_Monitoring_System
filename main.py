import logging
from config import config
from utils import decorators
from kafka_tools.kafka_tools import KafkaTools
from utils.decorators import log



logging.basicConfig(level=logging.DEBUG)
logging.getLogger('kafka').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    logger.info(" ____ Starting the application ____ ")
    
    producer = KafkaTools.Producer(bootstrap_servers=config.BOOTSTRAP_SERVERS)
    producer.publish_message(topic="test", message={"message_1": "Hello World!"})
    producer.publish_many_by_topics({"test": [{"message_3": "Hello World!"}, {"message_2": "Hello World!"}]})
    
    consumer = KafkaTools.Consumer.get_consumer("test", bootstrap_servers=config.BOOTSTRAP_SERVERS, group_id="test")

    for message in consumer:
        print("topic: ", message.topic)
        print("message: " , message.value)
        

if __name__ == "__main__":
    main()

