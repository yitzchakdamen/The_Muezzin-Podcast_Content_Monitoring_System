from kafka import KafkaConsumer, KafkaProducer
import json
import logging
from typing import Any
import datetime
from utils.decorators import safe_execute, log

logger = logging.getLogger(__name__)

class KafkaTools:
    
    class Producer:
        
        _instance = None
        _initialized = False

        def __new__(cls, *args, **kwargs):
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance
        
        @safe_execute()
        def __init__(self, bootstrap_servers: str):
            if KafkaTools.Producer._initialized: return
            self.producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda x: json.dumps(x, default=lambda x: str(x)).encode('utf-8')
                )
            logger.info("Producer object initialized.")
            KafkaTools.Producer._initialized = True
            
        @safe_execute(return_strategy="error")
        def publish_message(self, topic:str, message:Any, key=None):
            """Publish a message to a Kafka topic."""
            self.producer.send(topic, key=key, value=message)
        
        @safe_execute(return_strategy="error")
        def publish_many_by_topics(self, topics:dict[str,Any]):
            """Publish data to Kafka."""
            logger.info("Publishing data to Kafka ..")
            
            for topic, messages in topics.items():
                for message in messages:
                    self.publish_message(topic=topic, key=None, message=message)
                    logger.info(f"Published message: {str(message)} to topic: {str(topic)}")
                self.producer.flush()
    
    class Consumer:
        
        @staticmethod
        @safe_execute(return_strategy="error")
        def get_consumer(*topic:str, bootstrap_servers:str, group_id:str) -> KafkaConsumer:
            """Create a Kafka consumer."""
            logger.info("Creating Consumer Object ..")
            return KafkaConsumer(
                *topic,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                bootstrap_servers=[bootstrap_servers],
                auto_offset_reset='earliest'
            )
            
            
            
if __name__ == "__main__":
    producer = KafkaTools.Producer(bootstrap_servers="localhost:9092")
    producer.publish_message(topic="test", message={"message_1": "Hello World!"})
    producer.publish_many_by_topics({"test": [{"message_3": "Hello World!"}, {"message_2": "Hello World!"}]})
    
    consumer = KafkaTools.Consumer.get_consumer("test", bootstrap_servers="localhost:9092", group_id="test")

    for message in consumer:
        print("topic: ", message.topic)
        print("message: " , message.value)
        print("timestamp: ", datetime.datetime.fromtimestamp(message.timestamp / 1000))
        