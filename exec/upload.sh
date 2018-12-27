#!/usr/bin/env bash

#set paths
SCRIPT_DIR=$(cd $(dirname $(readlink -f $0 || echo $0));pwd -P)
BASE_SETTING="$SCRIPT_DIR/../setting"
CONF_DIR="$SCRIPT_DIR/../lambda_conf"
UPLOAD_DIR="$SCRIPT_DIR/../upload_env"
ENDPOINT_DIR="$SCRIPT_DIR/../endpoint"

PROFILE=$1  # Please select pro or dev
ENDPOINT=$2 # Please chose an endpoint to upload

ENDPOINTS=(`cat $SCRIPT_DIR/../endpoints.txt`)

if [ "$ENDPOINT" = all ]; then
  for endpoint in ${ENDPOINTS[@]}
  do
    # copy files to upload_env
    cat $CONF_DIR/${PROFILE}/lambda_${endpoint}.json > $UPLOAD_DIR/lambda.json
    cp -r $ENDPOINT_DIR/modules $UPLOAD_DIR
    cp -r $ENDPOINT_DIR/conf $UPLOAD_DIR
    cp $BASE_SETTING/requirements.txt $UPLOAD_DIR

    # copy python codes
    touch $UPLOAD_DIR/${endpoint}.py && cat $ENDPOINT_DIR/${endpoint}.py > $UPLOAD_DIR/${endpoint}.py
    cd $UPLOAD_DIR
    lambda-uploader  --role
  done
else
  # copy files to upload_env
  cat $CONF_DIR/${PROFILE}/lambda_$ENDPOINT.json > $UPLOAD_DIR/lambda.json
  cp -r $ENDPOINT_DIR/modules $UPLOAD_DIR
  cp -r $ENDPOINT_DIR/conf $UPLOAD_DIR
  cp $BASE_SETTING/requirements.txt $UPLOAD_DIR

  # copy python codes
  touch $UPLOAD_DIR/$ENDPOINT.py && cat $ENDPOINT_DIR/$ENDPOINT.py > $UPLOAD_DIR/$ENDPOINT.py
  cd $UPLOAD_DIR
  lambda-uploader  --role
fi
