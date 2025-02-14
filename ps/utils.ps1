# List S3 file names and sort
aws s3 ls s3://sds-prod-ingest-external-sftp-files/adyen-sftp-data/root/ --profile prod --region eu-west-1 | ConvertFrom-String | Where-Object P4 -Like "*received*" | Sort-Object -Property P4| Select-Object -Property P4

# update table owner
aws dynamodb update-item --table-name sds-prod-common-security-TablesMetadata --key '{\"table_name\": {\"S\": \"get_feedback_survey_detailed_survey_ordered_components_grid_items\"}, \"database_name\": {\"S\": \"customer_shop\"}}' --attribute-updates '{\"data_owner\": {\"Value\": {\"S\": \"c8464\"},\"Action\": \"PUT\"}}' --profile prod --region "eu-west-1"

# get athena query by id
aws athena get-query-execution --query-execution-id 2469525a-3c8a-4858-b7c3-6916965ab1d9 --profile prod --region eu-west-1

# sso
./venv/Scripts/python.exe src/base/sso2aws.py