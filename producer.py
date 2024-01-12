from decimal import Decimal
from time import sleep
from uuid import uuid4, UUID
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import math

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback, the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to the delivery
        report callback, we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))



# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://your-schema-registry-url',
  'basic.auth.user.info': '{}:{}'.format('your-schema-registry-username', 'your-schema-registry-password')
})

# Fetch the latest Avro schema for the value
subject_name = 'topic_name-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': 'your_bootstrap_servers',
    'security.protocol': 'your_security_protocol',
    'sasl.mechanisms': 'your_sasl_mechanisms',
    'sasl.username': 'your_sasl_username',
    'sasl.password': 'your_sasl_password',
    'key.serializer': 'key_serializer',  # Key will be serialized as a string
    'value.serializer': 'avro_serializer'  # Value will be serialized as Avro
})

df = pd.read_csv("delivery_trip_truck_data.csv", na_values=['na', 'null', 'NA', 'NULL'])

def convert_row_to_dict(row):
    converted_data = {}

    for column in row.index:
        value = row[column]

        if pd.notna(value):
            if column in {"Curr_lat", "Curr_lon"}:
                converted_data[column] = float(value)
            elif column in {"TRANSPORTATION_DISTANCE_IN_KM", "Minimum_kms_to_be_covered_in_a_day"}:
                converted_data[column] = int(value)
            else:
                converted_data[column] = str(value)
        else:
            converted_data[column] = None

    return converted_data

# Iterate over rows and produce to Kafka
for index, row in df.iterrows():
    record = convert_row_to_dict(row) 
    producer.produce(topic='topic_name', key=str(index), value=record, on_delivery=delivery_report)
    producer.flush()
    index += 1
