#!/usr/bin/env bash

#set paths
SCRIPT_DIR=$(cd $(dirname $(readlink -f $0 || echo $0));pwd -P)
BASE_TEXT="$SCRIPT_DIR/../setting/base_text"
CONF_DIR="$SCRIPT_DIR/../lambda_conf"
ENDPOINT_DIR="$SCRIPT_DIR/../endpoint"
SETTING_DIR="$SCRIPT_DIR/../setting"
ARCHIVE_DIR="$SCRIPT_DIR/../exec/archive"

ENDPOINT=$1 # Please name endpoint decently

ENDPOINTS=(`cat $SCRIPT_DIR/../endpoints.txt`)

for ep in "${ENDPOINTS[@]}";do
    [[ $ENDPOINT != "$ep" ]] && echo "This endpoint is a new one." && exit 1
done


sed_in_ec2(){
sed -i "s/${ENDPOINT}//g" $SCRIPT_DIR/../endpoints.txt
}

sed_in_local(){
sed -i.bk "s/${ENDPOINT}//g" $SCRIPT_DIR/../endpoints.txt
rm $SCRIPT_DIR/../endpoints.txt.bk
}

if [[ "$HOME" = */home/ec2-user/* ]]; then
sed_in_ec2
else
sed_in_local
fi


archive_lambda() {
mv $CONF_DIR/$1/lambda_$ENDPOINT.json $ARCHIVE_DIR/lambda_conf/$1/lambda_$ENDPOINT.json
}

mv $ENDPOINT_DIR/$ENDPOINT.py $ARCHIVE_DIR/endpoint/$ENDPOINT.py
archive_lambda dev
archive_lambda pro
