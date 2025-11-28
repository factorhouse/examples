#!/usr/bin/env bash

SCRIPT_PATH="$(cd $(dirname "$0"); pwd)"
DEPS_PATH=$SCRIPT_PATH/deps

rm -rf $DEPS_PATH && mkdir $DEPS_PATH

echo "Set-up environment..."

echo "Downloading MSK data generator..."
mkdir -p $DEPS_PATH/connector/msk-datagen
curl --silent -L -o $DEPS_PATH/connector/msk-datagen/msk-data-generator.jar \
  https://github.com/awslabs/amazon-msk-data-generator/releases/download/v0.4.0/msk-data-generator-0.4-jar-with-dependencies.jar


echo "Create Schema Registry configs..."
mkdir -p $DEPS_PATH/schema
cat << 'EOF' > $DEPS_PATH/schema/schema_jaas.conf
schema {
         org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
         debug="true"
         file="/etc/schema/schema_realm.properties";
     };
EOF

cat << 'EOF' > $DEPS_PATH/schema/schema_realm.properties
admin: CRYPT:adpexzg3FUZAk,schema-admin
EOF

echo "Environment setup completed."