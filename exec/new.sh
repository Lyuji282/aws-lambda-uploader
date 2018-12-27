#!/usr/bin/env bash

#set paths
SCRIPT_DIR=$(cd $(dirname $(readlink -f $0 || echo $0));pwd -P)
BASE_TEXT="$SCRIPT_DIR/../setting/base_text"
CONF_DIR="$SCRIPT_DIR/../lambda_conf"
ENDPOINT_DIR="$SCRIPT_DIR/../endpoint"
SETTING_DIR="$SCRIPT_DIR/../setting"

ENDPOINT=$1 # Please name endpoint decently

ENDPOINTS=(`cat $SCRIPT_DIR/../endpoints.txt`)

for ep in "${ENDPOINTS[@]}";do
    [[ $ENDPOINT == "$ep" ]] && echo "This endpoint is already used." && exit 1
done

echo "$ENDPOINT" | sudo tee -a $SCRIPT_DIR/../endpoints.txt

create_lambda() {
touch $CONF_DIR/$1/lambda_$ENDPOINT.json
touch $ENDPOINT_DIR/$ENDPOINT.py
cat $BASE_TEXT/lambda_json.txt > $CONF_DIR/$1/lambda_$ENDPOINT.json
cat $BASE_TEXT/lambda_handler.py > $ENDPOINT_DIR/$ENDPOINT.py
}

sed_in_ec2(){
sed -i "s/rr0rr/${1}/g" $CONF_DIR/$1/lambda_$ENDPOINT.json
sed -i "s/rr1rr/${ENDPOINT}/g" $CONF_DIR/$1/lambda_$ENDPOINT.json
sed -i "s/rrendpointrr/${ENDPOINT}/g" $ENDPOINT_DIR/$ENDPOINT.py
}

sed_in_local(){
sed -i.bk "s/rr0rr/${1}/g" $CONF_DIR/$1/lambda_$ENDPOINT.json
sed -i.bk  "s/rr1rr/${ENDPOINT}/g" $CONF_DIR/$1/lambda_$ENDPOINT.json
rm $CONF_DIR/$1/lambda_$ENDPOINT.json.bk
sed -i.bk  "s/rrendpointrr/${ENDPOINT}/g" $ENDPOINT_DIR/$ENDPOINT.py
rm $ENDPOINT_DIR/$ENDPOINT.py.bk
}

if [[ "$HOME" = */home/ec2-user/* ]]; then
create_lambda dev
sed_in_ec2 dev
create_lambda pro
sed_in_ec2 pro
else
create_lambda dev
sed_in_local dev
create_lambda pro
sed_in_local pro
fi


