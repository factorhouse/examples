#!/usr/bin/env bash

usage() {
  echo "Usage: $0 [-h] [S3_BUCKET_NAME]"
  echo ""
  echo "This script downloads necessary Kafka connector JARs and syncs them to an AWS S3 bucket."
  echo "The JARs will be uploaded to the the bucket."
  echo ""
  echo "Arguments:"
  echo "  S3_BUCKET_NAME   Optional. The name of the S3 bucket to sync to."
  echo "                   Defaults to 'factorhouse-instaclustr-custom-connector-bucket'."
  echo "  -h, --help       Display this help message and exit."
  exit 0
}

progress_bar() {
  local progress=$((CURRENT_STEP * 100 / TOTAL_STEPS))
  local done=$((progress / 2))
  local left=$((50 - done))
  local fill=$(printf "%${done}s")
  local empty=$(printf "%${left}s")
  echo -ne "\r⏳ Progress : [${fill// /#}${empty// /-}] ${progress}%"
}

flag_time_taken() {
  local end_time=$(date +%s)
  local elapsed=$((end_time - START_TIME))
  local minutes=$((elapsed / 60))
  local seconds=$((elapsed % 60))
  echo ""
  echo "✅ Download complete in ${minutes}m ${seconds}s!"
}

### Determine S3 bucket name from argument or default
S3_BUCKET_NAME="factorhouse-instaclustr-custom-connector-bucket" # default

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  usage
fi

if [ -n "$1" ]; then
  S3_BUCKET_NAME="$1"
fi

### Determine absolute paths for kafka/flink connectors
SCRIPT_PATH="$(cd $(dirname "$0"); pwd)"
SRC_PATH=$SCRIPT_PATH/src

### Delete the existing folder
rm -rf $SRC_PATH && mkdir $SRC_PATH

####
#### Kafka connectors
####
echo "▶️  Downloading and syncing Kafka connector artifacts to s3://${S3_BUCKET_NAME}/"

START_TIME=$(date +%s)
TOTAL_STEPS=3
CURRENT_STEP=0

curl --silent -o $SRC_PATH/confluent.zip \
  https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.6.5/confluentinc-kafka-connect-s3-10.6.5.zip \
  && unzip -qq $SRC_PATH/confluent.zip -d $SRC_PATH \
  && mv $SRC_PATH/confluentinc-kafka-connect-s3-10.6.5/lib $SRC_PATH/confluent-s3 \
  && rm $SRC_PATH/confluent.zip \
  && rm -rf $SRC_PATH/confluentinc-kafka-connect-s3-10.6.5
((CURRENT_STEP++)); progress_bar

mkdir -p $SRC_PATH/msk-datagen-with-smt
curl --silent -L -o $SRC_PATH/msk-datagen-with-smt/msk-data-generator.jar \
  https://github.com/awslabs/amazon-msk-data-generator/releases/download/v0.4.0/msk-data-generator-0.4-jar-with-dependencies.jar
curl --silent -L -o $SRC_PATH/msk-datagen-with-smt/unwrap-union-1.0.jar \
  https://github.com/factorhouse/factorhouse-local/raw/refs/heads/main/resources/kpow/plugins/unwrap-union-transform/unwrap-union-1.0.jar
((CURRENT_STEP++)); progress_bar

aws s3 sync "$SRC_PATH/" "s3://${S3_BUCKET_NAME}/" --quiet
((CURRENT_STEP++)); progress_bar
if [ $? -eq 0 ]; then
    echo "✅ Sync complete!"
else
    echo "❌ Sync failed. Please check your AWS credentials and S3 bucket permissions."
    exit 1
fi

flag_time_taken

echo ""