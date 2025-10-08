#### Deploy an EKS cluster
eksctl create cluster -f manifests/eks/cluster.eksctl.yaml

#### Deploy a Kafka cluster using the Strimzi operator
STRIMZI_VERSION="0.45.1"
DOWNLOAD_URL=https://github.com/strimzi/strimzi-kafka-operator/releases/download/$STRIMZI_VERSION/strimzi-cluster-operator-$STRIMZI_VERSION.yaml
curl -L -o manifests/kafka/strimzi-cluster-operator-$STRIMZI_VERSION.yaml ${DOWNLOAD_URL}

# Update namespace to 'kafka'
sed -i 's/namespace: .*/namespace: kafka/' manifests/kafka/strimzi-cluster-operator-$STRIMZI_VERSION.yaml

## Deploy a Kafka cluster
kubectl create -f manifests/kafka/kafka-cluster.yaml -n kafka

#### Deploy Kpow
## Deploy Kpow configs
kubectl apply -f manifests/kpow/config-files.yaml \
  -f manifests/kpow/config.yaml -n factorhouse

## Kpow annual
export HELM_EXPERIMENTAL_OCI=1
aws ecr get-login-password \
    --region us-east-1 | helm registry login \
    --username AWS \
    --password-stdin 709825985650.dkr.ecr.us-east-1.amazonaws.com

mkdir -p awsmp-chart && cd awsmp-chart
helm pull oci://709825985650.dkr.ecr.us-east-1.amazonaws.com/factor-house/kpow-aws-annual
tar xf $(pwd)/* && find $(pwd) -maxdepth 1 -type f -delete

cd ..
helm install kpow-annual ./awsmp-chart/kpow-aws-annual/ \
    -n factorhouse \
    --set serviceAccount.create=false \
    --set serviceAccount.name=kpow-annual \
    --values ./values/eks-annual.yaml

# kubectl -n factorhouse port-forward service/kpow-annual-kpow-aws-annual 3000:3000

## Kpow hourly
export HELM_EXPERIMENTAL_OCI=1
aws ecr get-login-password \
    --region us-east-1 | helm registry login \
    --username AWS \
    --password-stdin 709825985650.dkr.ecr.us-east-1.amazonaws.com

mkdir -p awsmp-chart && cd awsmp-chart
helm pull oci://709825985650.dkr.ecr.us-east-1.amazonaws.com/factor-house/kpow-aws-hourly
tar xf $(pwd)/* && find $(pwd) -maxdepth 1 -type f -delete

# Deploy Kpow hourly
cd ..
helm install kpow-hourly ./awsmp-chart/kpow-aws-hourly/ \
  -n factorhouse \
  --set serviceAccount.create=false \
  --set serviceAccount.name=kpow-hourly \
  --values ./values/eks-hourly.yaml

# kubectl -n factorhouse port-forward service/kpow-hourly-kpow-aws-hourly 3001:3000

#### Delete resources
## Delete the Kafka cluster and Strimzi operator
STRIMZI_VERSION="0.45.1"
kubectl delete -f manifests/kafka/kafka-cluster.yaml -n kafka
kubectl delete -f manifests/kafka/strimzi-cluster-operator-$STRIMZI_VERSION.yaml -n kafka

## Kpow and config maps
helm uninstall kpow-annual kpow-hourly -n factorhouse
kubectl delete -f manifests/kpow/config-files.yaml \
  -f manifests/kpow/config.yaml -n factorhouse

## EKS cluster
eksctl delete cluster -f manifests/eks/cluster.eksctl.yaml
