import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import json
import time
from pymongo import MongoClient
import hashlib



# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'your_bootstrap_servers',
    'sasl.mechanisms': 'your_sasl_mechanisms',
    'security.protocol': 'your_security_protocol',
    'sasl.username': 'your_sasl_username',
    'sasl.password': 'your_sasl_password',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://your-schema-registry-url',
  'basic.auth.user.info': '{}:{}'.format('your-schema-registry-username', 'your-schema-registry-password')
})

# Mongodb atlas connection
client = MongoClient(your_mongodb_uri)
db = client['database_name']
collection = db['collection_name']

# Fetch the latest Avro schema for the value
subject_name = 'delivery_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset'],
    'enable.auto.commit': True
})

# Subscribe to the  topic
consumer.subscribe(['topic_name'])

# Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)  # How many seconds to wait for message

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

        original_dict = msg.value()

        # Assuming msg.value() is a dictionary
        content_hash = hashlib.md5(json.dumps(original_dict, sort_keys=True).encode()).hexdigest()

        original_dict['content_hash'] = content_hash

        # Check if a record with the same content hash already exists in the MongoDB collection
        existing_record = collection.find_one({'content_hash': content_hash})

        if existing_record is None:
            collection.insert_one(original_dict)
        else:
            print("Record with the same content already exists in MongoDB. Skipping...")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
