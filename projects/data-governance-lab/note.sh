unset KPOW_SUFFIX
unset FLEX_SUFFIX
export LICENSE_PREFIX="trial"
export KPOW_LICENSE=/home/jaehyeon/.license/kpow/$LICENSE_PREFIX-license.env
export FLEX_LICENSE=/home/jaehyeon/.license/flex/$LICENSE_PREFIX-license.env

export ST_LICENSE_PREFIX="community"
export ST_LICENSE=/home/jaehyeon/.license/shadowtraffic/$ST_LICENSE_PREFIX-license.env

#### start environment
docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d \
  && docker compose -p stripped -f ./projects/data-governance-lab/compose-stripped.yml up -d \
  && docker compose -p store --profile clickhouse -f ./factorhouse-local/compose-store.yml up -d \
  && docker compose -p metadata --profile omt -f ./factorhouse-local/compose-metadata.yml up -d

#### deploy data generator
docker exec postgres rm -rf /tmp/ddl.sql \
  && docker cp ./projects/data-governance-lab/datagen/shadowtraffic-ddl.sql postgres:/tmp/ddl.sql \
  && docker exec postgres bash -c "PGPASSWORD=db_password psql -U db_user -d fh_dev -f /tmp/ddl.sql" \
  && docker compose -p st -f ./projects/data-governance-lab/datagen/compose-st.yml up -d

#### deploy kafka connect

#### deploy flink job
docker compose -p flex -f ./projects/data-governance-lab/compose-flink.yml up -d

#### create iceberg tables
docker cp projects/data-governance-lab/spark-stats-aggregator/iceberg_ddl.py \
  spark-iceberg:/tmp/iceberg_ddl.py

docker exec spark-iceberg /opt/spark/bin/spark-submit /tmp/iceberg_ddl.py

#### create OMT services and pipelines
python -m venv venv
source venv/bin/activate
pip install -r projects/data-governance-lab/requirements.txt

export JWT_TOKEN="<ingestion-bot-token>"
python projects/data-governance-lab/omt-manage/run.py -a create

#### run spark app
docker cp projects/data-governance-lab/spark-stats-aggregator/aggregator.py \
  spark-iceberg:/tmp/aggregator.py

docker exec -e JWT_TOKEN=$JWT_TOKEN \
  spark-iceberg /opt/spark/bin/spark-submit \
  /tmp/aggregator.py

#### delete environment
docker compose -p st -f ./projects/data-governance-lab/datagen/compose-st.yml down \
  && docker compose -p flex -f ./projects/data-governance-lab/compose-flink.yml down \
  && docker compose -p metadata --profile omt -f ./factorhouse-local/compose-metadata.yml down \
  && docker compose -p store --profile clickhouse -f ./factorhouse-local/compose-store.yml down \
  && docker compose -p stripped -f ./projects/data-governance-lab/compose-stripped.yml down \
  && docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml down
