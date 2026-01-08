unset KPOW_SUFFIX
unset FLEX_SUFFIX
export LICENSE_PREFIX="trial"
export KPOW_LICENSE=/home/jaehyeon/.license/kpow/$LICENSE_PREFIX-license.env
export FLEX_LICENSE=/home/jaehyeon/.license/flex/$LICENSE_PREFIX-license.env

export ST_LICENSE_PREFIX="community"
export ST_LICENSE=/home/jaehyeon/.license/shadowtraffic/$ST_LICENSE_PREFIX-license.env

docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d \
  && docker compose -p flex -f ./factorhouse-local/compose-flex.yml up -d \
  && docker compose -p metadata --profile omt -f ./factorhouse-local/compose-metadata.yml up -d

USE_EXT=false docker compose -p flex -f ./factorhouse-local/compose-flex.yml up -d postgres

docker exec postgres rm -rf /tmp/ddl.sql \
  && docker cp ./projects/data-governance-lab/datagen/shadowtraffic-ddl.sql postgres:/tmp/ddl.sql \
  && docker exec postgres bash -c "PGPASSWORD=db_password psql -U db_user -d fh_dev -f /tmp/ddl.sql"

docker compose -p st -f ./projects/data-governance-lab/datagen/compose-st.yml up -d

docker compose -p st -f ./projects/data-governance-lab/datagen/compose-st.yml down \
  && USE_EXT=false docker compose -p flex -f ./factorhouse-local/compose-flex.yml down

# docker run --name shadowtraffic -d \
#   --env-file $ST_LICENSE \
#   --network factorhouse \
#   -v ./projects/data-governance-lab/datagen/shadowtraffic.json:/home/config.json \
#   shadowtraffic/shadowtraffic:latest \
#   --config /home/config.json
  #  \
  # --watch --stdout --sample 10

# USE_EXT=false docker compose -p flex -f ./factorhouse-local/compose-flex.yml up -d postgres \
#   && docker compose -p metadata --profile omt -f ./factorhouse-local/compose-metadata.yml up -d

docker compose -p st -f ./projects/data-governance-lab/datagen/compose-st.yml down \
  && docker compose -p metadata --profile omt -f ./factorhouse-local/compose-metadata.yml down \
  && docker compose -p flex -f ./factorhouse-local/compose-flex.yml down \
  && docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml down
