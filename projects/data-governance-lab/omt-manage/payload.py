"""
Stores the API Payloads and Category mappings.
"""

# Update this to match your installed package path
SOURCE_PYTHON_CLASS = "custom_flink.metadata.CustomFlinkSource"

DATA = {
    # --- Standard AutoPilot Services ---
    "postgres": {
        "category": "databaseServices",
        "entityType": "databaseService",
        "ingestion_strategy": "autopilot",
        "json": {
            "name": "fh_local_pg",
            "serviceType": "Postgres",
            "connection": {
                "config": {
                    "type": "Postgres",
                    "scheme": "postgresql+psycopg2",
                    "username": "db_user",
                    "authType": {"password": "db_password"},
                    "hostPort": "postgres:5432",
                    "database": "fh_dev",
                    "supportsMetadataExtraction": True,
                }
            },
        },
    },
    "kafka": {
        "category": "messagingServices",
        "entityType": "messagingService",
        "ingestion_strategy": "autopilot",
        "json": {
            "name": "fh_local_kafka",
            "serviceType": "Kafka",
            "connection": {
                "config": {
                    "type": "Kafka",
                    "bootstrapServers": "kafka-1:19092,kafka-2:19093,kafka-3:19094",
                    "schemaRegistryURL": "http://schema:8081",
                    "supportsMetadataExtraction": True,
                }
            },
        },
    },
    "connect": {
        "category": "pipelineServices",
        "entityType": "pipelineService",
        "ingestion_strategy": "autopilot",
        "json": {
            "name": "fh_local_connect",
            "serviceType": "KafkaConnect",
            "connection": {
                "config": {
                    "type": "KafkaConnect",
                    "hostPort": "http://connect:8083",
                    "verifySSL": False,
                    "supportsMetadataExtraction": True,
                }
            },
        },
    },
    "clickhouse": {
        "category": "databaseServices",
        "entityType": "databaseService",
        "ingestion_strategy": "autopilot",
        "json": {
            "name": "fh_local_clickhouse",
            "serviceType": "Clickhouse",
            "connection": {
                "config": {
                    "type": "Clickhouse",
                    "scheme": "clickhouse+http",
                    "username": "default",
                    "hostPort": "ch-server:8123",
                    "supportsMetadataExtraction": True,
                }
            },
        },
    },
    "iceberg": {
        "category": "databaseServices",
        "entityType": "databaseService",
        "ingestion_strategy": "autopilot",
        "json": {
            "name": "fh_local_iceberg",
            "serviceType": "Iceberg",
            "connection": {
                "config": {
                    "type": "Iceberg",
                    "catalog": {
                        "name": "demo_ib",
                        "connection": {
                            "uri": "thrift://hive-metastore:9083",
                            "fileSystem": {
                                "type": {
                                    "awsAccessKeyId": "admin",
                                    "awsSecretAccessKey": "password",
                                    "awsRegion": "us-east-1",
                                    "endPointURL": "http://minio:9000",
                                }
                            },
                        },
                        "warehouseLocation": "s3://warehouse",
                    },
                    "supportsMetadataExtraction": True,
                }
            },
        },
    },
    # --- Custom Flink Pipeline ---
    "flink": {
        "category": "pipelineServices",
        "entityType": "pipelineService",
        "ingestion_strategy": "standard",  # Triggers create_custom_pipeline
        "json": {
            "name": "fh_local_flink",
            "description": "Custom Flink Pipeline Connector",
            "serviceType": "CustomPipeline",
            "connection": {
                "config": {
                    "type": "CustomPipeline",
                    "sourcePythonClass": SOURCE_PYTHON_CLASS,
                    "connectionOptions": {
                        "hostPort": "http://jobmanager:8081",
                        "verifySSL": "no-ssl",
                        # Define as Dict; run.py will jsonify it
                        "custom_lineage_config": {
                            "OrderProcessor": {
                                "upstream": [
                                    {
                                        "fqn": 'fh_local_kafka."ecomm.demo.orders"',
                                        "type": "topic",
                                    },
                                    {
                                        "fqn": 'fh_local_kafka."ecomm.demo.users"',
                                        "type": "topic",
                                    },
                                ],
                                "downstream": [
                                    {
                                        "fqn": "fh_local_iceberg.default.dev.orders_enriched",
                                        "type": "table",
                                    },
                                    {
                                        "fqn": "fh_local_clickhouse.default.dev.orders_enriched",
                                        "type": "table",
                                    },
                                ],
                            }
                        },
                    },
                }
            },
        },
        # Dedicated Ingestion Config
        "ingestion_payload": {
            "name": "flink_custom_lineage_ingestion",
            "pipelineType": "metadata",
            "airflowConfig": {"scheduleInterval": "@daily"},
            "sourceConfig": {
                "config": {
                    "source": {
                        "type": "CustomPipeline",
                        # serviceName injected by run.py
                        "sourceConfig": {"config": {"type": "PipelineMetadata"}},
                    },
                    "sink": {"type": "metadata-rest", "config": {}},
                    "workflowConfig": {
                        # Host and JWT injected by run.py
                        "openMetadataServerConfig": {
                            "authProvider": "openmetadata",
                        }
                    },
                }
            },
        },
    },
}
