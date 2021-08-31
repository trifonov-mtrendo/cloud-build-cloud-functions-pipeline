#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${DIR}/.env.local"
printf -v payload '{"account_id": "1900000034", "client_id": "1606794953", "access_token": "168951e1540a8ddd4dcd62c7b8b1161dcd074f2128d777dce1970aa1594e947949f5b4ae92e9740fd2596", "from": "2021-08-02", "to": "2021-08-02", "gbq_project": "westwing-gcp-apps", "gbq_dataset": "HOLOWAY_VK_Costs"}' 

MESSAGE_BASE64=$(echo -n $payload | base64)

EVENT_PAYLOAD=$(
  sed -e \
    "s|__DATA_BASE64_PLACEHOLDER__|${MESSAGE_BASE64}|g" \
    ./payloads/test-local-pubsub-payload.json
)

curl -X POST \
  -H'Content-type: application/json' \
  -d "${EVENT_PAYLOAD}" \
  "http://localhost:${FUNCTION_PORT_PUBSUB}"

echo
