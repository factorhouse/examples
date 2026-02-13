## Clone the Factor House Local Repository
git clone https://github.com/factorhouse/factorhouse-local.git

## Create a virtual environment
python -m venv venv
source venv/bin/activate
pip install -r features/offset-management/requirements.txt 

## Start Kafka + Kpow
# Community Edition
export KPOW_SUFFIX="-ce"
export KPOW_LICENSE=/home/jaehyeon/.license/kpow/community-license.env

docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d

# Enterprise Edition
unset KPOW_SUFFIX
export KPOW_LICENSE=/home/jaehyeon/.license/kpow/trial-license.env

docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d

## Start Kafka clients
# Terminal 1
python features/offset-management/producer.py 
# Terminal 2
python features/offset-management/simple_consumer.py

## Teardown Kafka + Kpow
docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml down
unset KPOW_SUFFIX