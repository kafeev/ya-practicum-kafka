import json
import sr_http  # noqa: F401  (monkeypatches Schema Registry client to use requests instead of httpx)

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.serialization import SerializationError

BROKERS = 'rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net:9091'
TOPIC = 'topic-1'
USER = 'practicumuser'
PASSWORD = 'SecurePass2026'
SR_URL = 'http://localhost:8081'
CA = 'YandexInternalRootCA.crt'

key_schema = {'type': 'string'}
value_schema = {
    'type': 'record',
    'name': 'Message',
    'namespace': 'practicum.kafka',
    'fields': [{'name': 'message', 'type': 'string'}],
}

sr = SchemaRegistryClient({'url': SR_URL})
key_deserializer = AvroDeserializer(sr, json.dumps(key_schema))
value_deserializer = AvroDeserializer(sr, json.dumps(value_schema))

consumer = Consumer({
    'bootstrap.servers': BROKERS,
    'group.id': 'avro-consumer',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': USER,
    'sasl.password': PASSWORD,
    'ssl.ca.location': CA,
    'auto.offset.reset': 'earliest',
})

consumer.subscribe([TOPIC])
print('Avro consumer started, waiting for messages...', flush=True)

while True:
    msg = consumer.poll(10)
    if msg is None:
        continue
    if msg.error():
        print('Consumer error: {}'.format(msg.error()))
        continue

    try:
        key = key_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
        value = value_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
    except SerializationError as e:
        print('Skipping non-Avro message: {}'.format(e))
        continue
    print('key={} value={}'.format(key, value))
