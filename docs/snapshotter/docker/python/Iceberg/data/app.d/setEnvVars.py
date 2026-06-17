import os

aws_region = os.environ.get("AWS_REGION")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
s3_endpoint = os.environ.get("S3_ENDPOINT")
catalog_uri = os.environ.get("CATALOG_URI")
warehouse_location = os.environ.get("WAREHOUSE_LOCATION")
