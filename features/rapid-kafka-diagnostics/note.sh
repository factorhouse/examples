export KPOW_SUFFIX="-ce"
export KPOW_LICENSE=/home/jaehyeon/.license/kpow/community-license.env

docker compose -f ./factorhouse-local/compose-kpow.yml up -d

docker compose -f ./features/rapid-kafka-diagnostics/compose-producer.yml up -d --build

docker compose -f ./features/rapid-kafka-diagnostics/compose-consumer.yml up -d consumer --scale consumer=3

## tear down
docker compose -f ./features/rapid-kafka-diagnostics/compose-consumer.yml down \
  && docker compose -f ./features/rapid-kafka-diagnostics/compose-producer.yml down \
  && docker compose -f ./factorhouse-local/compose-kpow.yml down

unset KPOW_SUFFIX
unset KPOW_LICENSE
