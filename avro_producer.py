import json
from confluent_kafka import Producer
import sr_http  # noqa: F401  (monkeypatches Schema Registry client to use requests instead of httpx)

from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

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

_subject_conf = {'subject.name.strategy': topic_subject_name_strategy, 'auto.register.schemas': False, 'use.latest.version': True}

key_serializer = AvroSerializer(sr, json.dumps(key_schema), conf=_subject_conf)
value_serializer = AvroSerializer(sr, json.dumps(value_schema), conf=_subject_conf)


def delivery_report(err, msg):
    if err is not None:
        print('Delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


producer = Producer({
    'bootstrap.servers': BROKERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': USER,
    'sasl.password': PASSWORD,
    'ssl.ca.location': CA,
})

producer.produce(
    topic=TOPIC,
    key=key_serializer('key-1', SerializationContext(TOPIC, MessageField.KEY)),
    value=value_serializer(
        {'message': 'hello from avro producer'},
        SerializationContext(TOPIC, MessageField.VALUE),
    ),
    on_delivery=delivery_report,
)
producer.flush()
print('Avro message sent')
