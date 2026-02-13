export KPOW_SUFFIX="-ce"
export KPOW_LICENSE=/home/jaehyeon/.license/kpow/community-license.env

docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d

docker compose -p producer -f ./features/rapid-kafka-diagnostics/compose-producer.yml up -d

docker compose -p consumer -f ./features/rapid-kafka-diagnostics/compose-consumer.yml up -d --scale consumer=6

## tear down
docker compose -p consumer -f ./features/rapid-kafka-diagnostics/compose-consumer.yml down \
  && docker compose -p producer -f ./features/rapid-kafka-diagnostics/compose-producer.yml down \
  && docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml down

unset KPOW_SUFFIX
unset KPOW_LICENSE
