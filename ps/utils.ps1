# List S3 file names and sort
aws s3 ls s3://sds-prod-ingest-external-sftp-files/adyen-sftp-data/root/ --profile prod --region eu-west-1 | ConvertFrom-String | Where-Object P4 -Like "*received*" | Sort-Object -Property P4| Select-Object -Property P4
