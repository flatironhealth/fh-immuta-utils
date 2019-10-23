# Copyright 2018 Immuta, Inc. Licensed under the Immuta Software License
# Version 0.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
#    http://www.immuta.com/licenses/Immuta_Software_License_0.1.txt
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import boto3
import json

from botocore.exceptions import ClientError
from flask import Flask, request, Response, jsonify


app = Flask(__name__)
BOTO_OBJECT_404 = "NoSuchKey"


@app.route("/health")
def health():
    return "ok", 200


@app.route("/list")
def listing():
    """Provides a /list to list all S3 buckets
    This is a misc helper for debug purposes
    """
    conn = boto3.resource("s3")
    buckets = conn.buckets.all()
    result = []
    for bucket in buckets:
        result.append(bucket.name)
    return json.dumps(result)


@app.route("/list/<bucket>")
def bucket_listing(bucket):
    """Provides a /list/<bucket name> to list all files within an S3 bucket
    This is a misc helper for debug purposes
    """
    conn = boto3.resource("s3")
    objects = conn.Bucket(bucket).objects.all()
    result = []
    for obj in objects:
        result.append(obj.key)
    return json.dumps(result)


@app.route("/blobs/<path:blob_id>")
def fetch(blob_id):
    """Fetches a blob from S3, streaming it to the client byte-by-byte"""
    try:
        bucket_name, file_name = blob_id.split(":", 1)
    except ValueError:
        return "Invalid blob_id: " + blob_id, 400

    key = boto3.resource("s3").Object(bucket_name, file_name)
    try:
        response = key.get()

        def generator():
            for chunk in iter(lambda: response["Body"].read(1024 * 8), b""):
                yield chunk

        return Response(
            generator(),
            mimetype=key.content_type,
            headers={"Content-Length": response["ContentLength"]},
        )
    except ClientError as e:
        code = 500
        if BOTO_OBJECT_404 == e.response["Error"].get("Code", "Unknown"):
            code = 404
        return (str(e), code)
