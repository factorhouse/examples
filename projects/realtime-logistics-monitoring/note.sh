unset KPOW_SUFFIX
unset FLEX_SUFFIX
export LICENSE_PREFIX="trial"
export KPOW_LICENSE=/home/jaehyeon/.license/kpow/$LICENSE_PREFIX-license.env
export FLEX_LICENSE=/home/jaehyeon/.license/flex/$LICENSE_PREFIX-license.env
export ST_LICENSE=/home/jaehyeon/.license/shadowtraffic/$LICENSE_PREFIX-license.env

docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d

docker run --rm \
  --env-file $ST_LICENSE \
  --network factorhouse \
  -v ./projects/realtime-logistics-monitoring/shipment-single.json:/home/config.json \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json \
  --watch --stdout --sample 10
