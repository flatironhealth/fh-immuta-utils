# BoData S3 Blob Handler

The S3 Blob Handler is a REST service written in python that can provide S3 blobs to Immuta for multiple data sources.

## AWS Configuration

AWS credentials are provided to the s3blobhandler in a JSON configuration file. The properties it uses are
__access_key_id__, __secret_access_key__, and __region__.

Pass as __--aws-config__ on the command line. Defaults to _~/aws_config.json_.

```json
{
  "access_key_id": "<access key id>",
  "secret_access_key": "<secret access key>",
  "region": "us-west-2"
}
```

## REST API

### List Buckets
```
GET /list
```

### List Bucket objects
```
GET /list/<bucket_name>
```

### Get Blobs
```
GET /<bucket_name>/<path:blob_key>
```

## Developing
Set up your development environment by installing the project requirements:

```
$ pip install -r requirements.txt
```
