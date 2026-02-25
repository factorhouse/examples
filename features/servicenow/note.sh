API_HOST=http://localhost:4000
API_HOST=https://hardened-proteolytic-armando.ngrok-free.dev
AUTH_HEADER=$(echo "Authorization: Basic $(echo -n 'admin:admin' | base64)")

CLUSTER_ID=$(curl -s -H "$AUTH_HEADER" $API_HOST/kafka/v1/clusters | jq .clusters[0].id)
POLICY_ID=$(curl -s -H "$AUTH_HEADER" $API_HOST/admin/v1/temporary-policies | jq -r .temporary_policies[0].id)

curl -H "$AUTH_HEADER" $API_HOST/admin/v1/temporary-policies

curl -H "$AUTH_HEADER" $API_HOST/admin/v1/temporary-policies/$POLICY_ID

curl -X DELETE -H "$AUTH_HEADER" $API_HOST/admin/v1/temporary-policies/$POLICY_ID

curl $API_HOST/admin/v1/temporary-policies \
  -X POST \
  -H 'Content-Type: application/json' \
  -H "$AUTH_HEADER" \
  -d '{
  "duration_ms": 3600000,
  "policy": {
    "role": "kafka-users",
    "actions": [
      "TOPIC_DATA_QUERY"
    ],
    "effect": "Allow",
    "resource": ["cluster", "42RglGpZQwy9D5VzTVpCWA", "topic", "__oprtr_audit_log"]
  }
}'



wget https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.zip
unzip ngrok-stable-linux-amd64.zip
chmod +x ngrok
sudo mv ngrok ~/.local/bin
ngrok http 4000