## SDS-10321
  ### SDS-10321-s3-to-event prod
    prod sds-{}-ingest-external-sftp-files sds-{}-ingest-external-sftp-files-out concardis-sftp-data 20
  ### SDS-10321-s3-split-by-date prod
    prod sds-prod-ingest-external-sftp-files-out "concardis-sftp-data/year=2024/month=04/day=19" "concardis-sftp-data" "\d{4}-\d{2}-\d{2}"
    

## SDS-11052
  ### SDS-11052-s3-to-event prod
    prod sds-{}-ingest-external-sftp-files amex-sftp-data sds-{}-ingest-external-sftp-files-out "amex-sftp-data/amex_submission" "ERROR/amex-sftp-data" 20
  ### SDS-11052-s3-split-by-date prod
    prod sds-prod-ingest-external-sftp-files-out "amex-sftp-data/amex_txnpricing/year=2024/month=07/day=10" "amex-sftp-data/amex_txnpricing" "\d{4}-\d{2}-\d{2}"      

## SDS-11330
  ### SDS-11330-s3-to-event prod
    prod sds-{}-ingest-external-sftp-files jpmorgan-sftp-data sds-{}-ingest-external-sftp-files-out "jpmorgan-sftp-data/jpmorgan_com_report/jp_morgan_com_chargeback_detail_record_french" "ERROR/jpmorgan-sftp-data/jpmorgan_com_report" 10    