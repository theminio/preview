import json
import os
import requests
import boto3
import botocore
import urllib3

from flask import Flask
from flask import json
from flask import request
from flask import send_from_directory
from flask import Response
from flask_cors import CORS
from botocore.client import Config

REGION = None
ENCODING_TYPE = "ISO-8859-1"
urllib3.disable_warnings()

URL_LIFE_TIME_SECONDS = 7200 # 2 hours

CLIENT_URL = os.environ.get("CLIENT_URL", "http://10.3.178.117:9000") ### THE CHALLENGE
MINIO_REGION = os.environ.get("MINIO_REGION", "prod")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://10.3.178.117:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "G5CU0G5CO7PJW19G05P0")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "5g6TvlWIKS0VLplN5wfB3Dc1sHRNfWclvCOnWC+c")

app=Flask(__name__, static_url_path='') ### THE CHALLENGE
CORS(app, resources={r"/*": {"origins": CLIENT_URL}})

def from_request(request, k):
    if not request.json:
        raise Exception("Invalid Request")
    return str(request.json[k])

def fetch_preview_request(request):
    bucket_name = from_request(request, "bucketName")
    folder_prefix = from_request(request, "folderPrefix")
    object_name = from_request(request, "objectName")

    return [bucket_name, folder_prefix, object_name]

def connect_to_s3_client():
    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name=MINIO_REGION,
                             use_ssl=False,
                             verify=False,
                             config=Config(signature_version='s3'))

    return s3_client

def connect_to_s3_resource():
    s3_resource= boto3.resource('s3',
                                endpoint_url=MINIO_ENDPOINT,
                                aws_access_key_id=MINIO_ACCESS_KEY,
                                aws_secret_access_key=MINIO_SECRET_KEY,
                                region_name=MINIO_REGION,
                                use_ssl=False,
                                verify=False,
                                config=Config(signature_version='s3'))

    return s3_resource

def parse_string_to_array(encoded_array):
    decoded_array, places_of_all_apostrophe = [], []
    start_from = 0
    
    while start_from != -1:
        start_from = encoded_array.find("'", start_from + 1)
        places_of_all_apostrophe.append(start_from)

    for i in range(len(places_of_all_apostrophe) // 2):
        from_index = places_of_all_apostrophe[i * 2] + 1
        to_index = places_of_all_apostrophe[i * 2 + 1]
        decoded_array.append(encoded_array[from_index:to_index])

    return decoded_array

def preview_csv_from_object(args):
    file = args[0]

    CHUNK_SIZE = 1024

    curr_start = 0
    curr_end = min(CHUNK_SIZE - 1, file.content_length - 1)
    if curr_end < 0:
        return [""]

    buffer_string = ""

    EOL = ["\r", "\n", "\r\n"]

    while not True in map(lambda eol : buffer_string.count(eol) >= 11, EOL):
        if curr_start >= file.content_length:
            break

        file_object = file.get(Range="bytes=" + str(curr_start) + "-" + str(curr_end))["Body"]
        curr_file_content = file_object.read()
        buffer_string += curr_file_content.decode(ENCODING_TYPE)
        curr_start += CHUNK_SIZE
        curr_end += min(CHUNK_SIZE, file.content_length - 1 - curr_end)

    for eol in EOL:
        if buffer_string.count(eol) > 0:
            while buffer_string.count(eol) >= 11:
                buffer_string = buffer_string[:-1]

            result = buffer_string.split(eol)

            return result

    if result[-1] == "":
        result = result[:-1]
    
    return result

def preview_txt_from_object(args):
    file = args[0]
    
    CHUNK_SIZE = 1024

    curr_start = 0
    curr_end = min(CHUNK_SIZE - 1, file.content_length - 1)

    if curr_end < 0:
        return ""

    file_object = file.get(Range="bytes=" + str(curr_start) + "-" + str(curr_end))["Body"]
    curr_file_content = file_object.read()
    buffer_string = curr_file_content.decode(ENCODING_TYPE)

    return buffer_string

def preview_avro_from_object(args):
    file = args[0]
    
    CHUNK_SIZE = 1024

    curr_start = 0
    curr_end = min(CHUNK_SIZE - 1, file.content_length - 1)

    if curr_end < 0:
        return ""

    file_object = file.get(Range="bytes=" + str(curr_start) + "-" + str(curr_end))["Body"]
    curr_file_content = file_object.read()
    buffer_bytes = curr_file_content
    buffer_string = buffer_bytes.decode(ENCODING_TYPE)

    count_bracket = 1
    schema_from_index = buffer_string.index('{')
    index = schema_from_index

    while count_bracket > 0:
        index += 1
        
        if index > curr_end:
            cur_start = curr_end + 1
            curr_end += min(CHUNK_SIZE, file.content_length -1 - curr_end)
            file_object = file.get(Range="bytes=" + str(cur_start) + "-" + str(curr_end)["BODY"])
            curr_file_content = file_object.read()
            buffer_bytes += curr_file_content
            buffer_string = buffer_bytes.decode(ENCODING_TYPE)

        if buffer_string[index] == '{':
            count_bracket += 1
        elif buffer_string[index] == '}':
            count_bracket -= 1
    
    schema_to_index = index + 1

    return buffer_string[schema_from_index:schema_to_index].replace("\\'", "'")

def preview_image_from_object(args):
    s3_client, bucket_name, object_name, folder_prefix = args

    url = s3_client.generate_presigned_url(
        ClientMethod = "get_object",
        Params = {
            "Bucket" : bucket_name,
            "Key" : folder_prefix + object_name,
            "ResponseContentType" : "application/octet-stream"
        },
        ExpiresIn = URL_LIFE_TIME_SECONDS
    )

    return str(url)

def read_from_object(s3_client, s3_resource, bucket_name, folder_prefix, object_name):
    file = s3_resource.Object(bucket_name, folder_prefix + object_name)

    images = ["mp4", "png", "jpeg", "jpg", "gif", "tif"]

    read_from_object_dict = {
        "csv" : [preview_csv_from_object, [file]],
        "avro" : [preview_avro_from_object, [file]],
        "img" : [preview_image_from_object, [s3_client, bucket_name, object_name, folder_prefix]]
    }

    split_by_dot = file.key.split(".")
    extension = split_by_dot[-1].lower()
    if extension in images:
        extension = "img"
    elif extension == "avsc":
        extension = "avro"

    if not extension in read_from_object_dict:
        extension = "txt"
        read_from_object_dict[extension] = [preview_txt_from_object, [file]]

    if len(split_by_dot) == 1 or extension not in read_from_object_dict:
        return 500, []

    read = read_from_object_dict[extension][0]
    read_args = read_from_object_dict[extension][1]

    return 200, read(read_args)

@app.route("/preview", methods = ["POST"]) # Small for Gift :)
def preview_object():
    print(request.json)

    bucket_name, folder_prefix, object_name = fetch_preview_request(request)

    s3_client = connect_to_s3_client()
    s3_resource = connect_to_s3_resource()

    response_status, data = read_from_object(s3_client, s3_resource, bucket_name, folder_prefix, object_name)

    resp = Response(response=json.dumps(data), status=response_status)
    resp.headers["Content-type"] = "applications/json; charset=UTF-8"

    return resp

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, threaded=True)