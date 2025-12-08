# OCI Streaming Integration

## OCI Streaming for Apache Kafka

```bash
docker run -d -p 3000:3000 --name kpow \
  -e ENVIRONMENT_NAME="OCI Kafka Cluster" \
  -e BOOTSTRAP="<BOOTSTRAP_SERVER_ADDRESS>" \
  -e SECURITY_PROTOCOL="SASL_SSL" \
  -e SASL_MECHANISM="SCRAM-SHA-512" \
  -e SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required username="<VAULT_USERNAME>" password="<VAULT_PASSWORD>";" \
  --env-file="<KPOW_LICENCE_FILE>" \
  factorhouse/kpow-ce:latest
```

## OCI Streaming

```bash
docker run -d -p 3000:3000 --name kpow \
  -e ENVIRONMENT_NAME="OCI Streaming" \
  -e BOOTSTRAP="<BOOTSTRAP_SERVER_ADDRESS>" \
  -e SECURITY_PROTOCOL="SASL_SSL" \
  -e SASL_MECHANISM="PLAIN" \
  -e SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="<TENANCY_NAME>/<USER_NAME>/<STREAM_POOL_OCID>" password="<OCI_AUTH_TOKEN>";' \
  -e PERSISTENCE_MODE="none" \
  --env-file="<KPOW_LICENCE_FILE>" \
  factorhouse/kpow-ce:latest
```

## Complete OCI Streaming for Apache Kafka Example

See this blog post - [Integrate Kpow with Oracle Compute Infrastructure (OCI) Streaming with Apache Kafka](https://factorhouse.io/how-to/integrate-kpow-with-oci-streaming)
