#!/bin/bash
set -e

DEPLOY_BUCKET='airflow-edw-poc'

# Push Code
echo "Push code"
(cd plugins; chmod -R 755 .;  zip -r ../plugins.zip .)
S3_Stub="s3://$DEPLOY_BUCKET"
aws s3 cp plugins.zip "$S3_Stub"
aws s3 cp requirements.txt "$S3_Stub"

aws s3 cp dags/ "$S3_Stub/dags/" --recursive