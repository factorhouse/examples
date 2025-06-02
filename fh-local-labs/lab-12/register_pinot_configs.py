import os
import json
import urllib3
import argparse

BASE_URL = os.getenv("BASE_URL", "http://localhost:19000")
OBJECT_NAME = "orders"

http = urllib3.PoolManager()


def _pinot_configs(object_name: str):
    """Returns Pinot schema and table configs of an object."""
    schema_config = {
        "schemaName": f"{object_name}",
        "dimensionFieldSpecs": [
            {"name": "supplier", "dataType": "STRING"},
            {"name": "item", "dataType": "STRING"},
            {"name": "price", "dataType": "STRING"},
            {"name": "order_id", "dataType": "STRING"},
        ],
        "dateTimeFieldSpecs": [
            {
                "name": "bid_time",
                "dataType": "LONG",
                "format": "1:MILLISECONDS:EPOCH",
                "granularity": "1:MILLISECONDS",
            }
        ],
    }
    table_config = {
        "tableName": f"{object_name}",
        "tableType": "REALTIME",
        "tenants": {"broker": "DefaultTenant", "server": "DefaultTenant"},
        "segmentsConfig": {
            "timeColumnName": "bid_time",
            "schemaName": f"{object_name}",
            "replication": "1",
        },
        "tableIndexConfig": {"loadMode": "MMAP"},
        "ingestionConfig": {
            "streamIngestionConfig": {
                "streamConfigMaps": [
                    {
                        "streamType": "kafka",
                        "stream.kafka.topic.name": f"{object_name}",
                        "stream.kafka.broker.list": "kafka-1:19092",
                        "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
                        "stream.kafka.decoder.prop.schema.registry.rest.url": "http://schema:8081",
                        "stream.kafka.decoder.prop.schema.registry.basic.auth.credentials.source": "USER_INFO",
                        "stream.kafka.decoder.prop.schema.registry.basic.auth.user.info": "admin:admin",
                        "stream.kafka.consumer.type": "simple",
                        "stream.kafka.consumer.prop.group.id": f"{object_name}-pinot-group",
                        "stream.kafka.offset.criteria": "SMALLEST",
                    }
                ]
            }
        },
        "metadata": {},
    }
    return schema_config, table_config


def upload_schema_and_table(http: urllib3.PoolManager, base_url: str, object_name: str):
    """Upload Pinot schema and table configs of an object."""
    schema_config, table_config = _pinot_configs(object_name)
    schema_response = http.request(
        "POST",
        f"{base_url}/schemas",
        body=json.dumps(schema_config),
        headers={"Content-Type": "application/json"},
    )
    print("Schema upload status:", schema_response.status)
    print("Schema response:", schema_response.data.decode())
    table_response = http.request(
        "POST",
        f"{base_url}/tables",
        body=json.dumps(table_config),
        headers={"Content-Type": "application/json"},
    )
    print("Table upload status:", table_response.status)
    print("Table response:", table_response.data.decode())


def delete_schema_and_table(http: urllib3.PoolManager, base_url: str, object_name: str):
    """Delete Pinot schema and table configs of an object."""
    table_response = http.request("DELETE", f"{base_url}/tables/{object_name}")
    print("Delete table status:", table_response.status)
    print("Delete table response:", table_response.data.decode())
    schema_response = http.request("DELETE", f"{base_url}/schemas/{object_name}")
    print("Delete schema status:", schema_response.status)
    print("Delete schema response:", schema_response.data.decode())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Register Pinot schema and table")
    parser.add_argument(
        "--action",
        "-a",
        type=str,
        choices=["delete", "upload"],
        default="upload",
        help="Action to perform. Either upload or delete, Default: upload",
    )
    args = parser.parse_args()

    if args.action == "delete":
        delete_schema_and_table(http, BASE_URL, OBJECT_NAME)
    else:
        upload_schema_and_table(http, BASE_URL, OBJECT_NAME)
