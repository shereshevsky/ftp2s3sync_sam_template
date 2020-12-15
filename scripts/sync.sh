cd /home/ubuntu/ftp2s3sync_sam_template/src
python3.8 app.py

aws s3 sync s3://dwh-load-bucket/DEV/FTP/IMS/FF_Belarus/ s3://dwh-load-bucket/upload/Merck_STG/Belarus/
aws s3 sync s3://dwh-load-bucket/DEV/FTP/IMS/ImsDataKaz/ s3://dwh-load-bucket/upload/Merck_STG/Kazakhstan/
aws s3 sync s3://dwh-load-bucket/DEV/FTP/IMS/FF_Russia/ s3://dwh-load-bucket/upload/Merck_STG/Russia/
aws s3 sync s3://dwh-load-bucket/DEV/FTP/IMS/RusIndex/ s3://dwh-load-bucket/upload/Merck_STG/Russia/
aws s3 sync s3://dwh-load-bucket/DEV/FTP/IMS/Ukraine/ s3://dwh-load-bucket/upload/Merck_STG/Ukraine/
