from kafka import KafkaAdminClient

admin = KafkaAdminClient(
    bootstrap_servers="rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net:9091",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username="registry",
    sasl_plain_password="registry1",
    ssl_cafile="YandexInternalRootCA.crt",
)

methods = [m for m in dir(admin) if "group" in m.lower() or "consum" in m.lower()]
print("Available group methods:", methods)

try:
    admin.delete_groups(group_ids=["schema-registry"])
    print("Deleted group 'schema-registry'")
except Exception as e:
    print("Delete failed:", repr(e))
