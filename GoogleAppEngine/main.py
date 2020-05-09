# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json
import logging
import os
import time
import threading
from flask import current_app, Flask, render_template, request, send_file, jsonify
from firebase_admin import db, credentials, firestore, initialize_app
from google.cloud import pubsub_v1
from google.cloud import storage
from flask_cors import CORS, cross_origin

# Configuring firebase
app = Flask(__name__)
CORS(app, support_credentials=True)
cred = credentials.Certificate('credentials.json')
initialize_app(cred, {
    'databaseURL': 'https://cdrive-dev.firebaseio.com/'
})
# Configure the following environment variables via app.yaml
# This is used in the push request handler to verify that the request came from
# pubsub and originated from a trusted source.
app.config['PUBSUB_VERIFICATION_TOKEN'] = 123123
#app.config['PUBSUB_TOPIC'] = os.environ['PUBSUB_TOPIC']
app.config['PROJECT'] = 'cc2020project2'

# Initialize the publisher client once to avoid memory leak
# and reduce publish latency.
publisher = pubsub_v1.PublisherClient()
BUCKET_NAME = 'cc2020project2_bucket'
# Initialize Google Cloud Storage
gcs_client = storage.Client()
bucket = gcs_client.bucket('cc2020project2_bucket')
URL_CACHE = 'https://storage.cloud.google.com/' + BUCKET_NAME + '/' 

'''
def test():
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    acl = bucket.acl
    acl.user("apoorv2arsenalfc@gmail.com").grant_read()
'''

@app.route('/', methods=['GET'])
def index():
    return "App running", 200
    
@cross_origin(supports_credentials=True)
@app.route('/upload', methods=['POST'])
def upload():

    body = request.get_json()
    user_id = request.form.get('userId')

    print ('API ENDPOINT: Upload')
    print ('User Id ', user_id)
    file_obj = request.files.get('file')

    file_obj_read=file_obj.read()
    file_obj.seek(0)
    file_size = len(file_obj_read)
    
    file_name = file_obj.filename
    original_file_name = file_name

    file_name = str(user_id) + file_name 
    print("Original File name: ", original_file_name)
    print("Secure File name: ", file_name)
    
    print("File size: ", file_size)

    # file_size = 10
    # Find available storage, update its space in DB

    storage_id = check_if_file_exists_das(user_id, original_file_name)
    if storage_id == None:
        print("FIREBASE: Finding Free Storage Device")
        storage_id = find_available_storage(file_size)
        if storage_id == None:
            # Not enough storage space
            print("FIREBASE: Not enough storage space available")
            return "SPACE_EXCEEDED", 200
        else:
            print("FIREBASE: Storage Id Found ", storage_id)

    else:
        print("FIREBASE: File already exists in storage Device", storage_id)

    topic = storage_id

    print("GCS: Uploading file to Cloud Storage...")
    url=upload_file(file_obj, file_name)
    print("GCS: UPLOAD SUCCESS")

    print("FIREBASE: Updating user space usage")
    update_user_quota(user_id,file_size, True)
    print("FIREBASE: UPDATE SUCCESS")
    
    print("FIREBASE: Updating File objects for user ", user_id)
    update_user_files(user_id,storage_id,original_file_name,file_size)
    print("FIREBASE: UPDATE SUCCESS")

    action = "download"
    payload = build_payload(action,file_name)
    
    print("PUBSUB: publishing message to topic ", topic)

    if topic != None:
        publish(topic, payload)
    print("PUBSUB: Message published")
    print("API SUCCESS: Returning status ", 200)
    return 'UPLOAD_COMPLETE', 200

@cross_origin(supports_credentials=True)
@app.route('/delete', methods=['POST'])
def delete():
    print ('API ENDPOINT: Delete')
    body = request.get_json()
    print (body)
    user_id = body['userId']
    file_name = body['file']
    print("User id: ", user_id)
    original_file_name = file_name
    
    print("Original File name: ", original_file_name)
    file_name = str(user_id) + file_name 
    
    print("Secure File name: ", file_name)
    #TODO DB Find on which device user's file is stored
    storage_id = find_storage_id(user_id, original_file_name)  
    topic = storage_id
    action = "delete"
    payload = build_payload(action, file_name)
    
    print("PUBSUB: publishing message to topic ", topic)
    publish(topic, payload)
    
    print("PUBSUB: Message published")
    #Updating Delete DATABASE
    print("FIREBASE: Deleting File object from User collections")
    file_size = delete_user_file_DB(user_id, original_file_name)
    print("FIREBASE: UPDATE SUCCESS", file_size)
    print("FIREBASE: Updating User Space Use")
    update_user_quota(user_id, file_size , False)
    print("FIREBASE: UPDATE SUCCESS")
    print("API: SUCCESS, Returning status ", 200)
    
    return 'DELETE_COMPLETE', 200

#TODO
@cross_origin(supports_credentials=True)
@app.route('/download', methods=['POST'])
def download():
    print ('API ENDPOINT: Download')

    body = request.get_json()
    user_id = body['userId']
    file_name = body['file']
    print("User Id: ", user_id)
    original_file_name = file_name
    print("Original File name: ", original_file_name)
    file_name = str(user_id) + file_name 
    
    print("Secure File name: ", file_name)

    if check_file_in_cache(file_name):
        return URL_CACHE+file_name, 200
    

    print("FIREBASE: Finding storage where the file is stored")
    storage_id = find_storage_id(user_id, original_file_name)
    
    if storage_id==None:
        #TODO ERROR HANDLING
        
        print("FIREBASE: No storage device available with enough free space")
        return 'No such file Exists!'
    else:
        print("FIREBASE: Storage id found ", storage_id)
    topic = storage_id
    
    action = "upload"
    
    print("PUBSUB: publishing message to topic ", topic)
    payload = build_payload(action,file_name)
    print("PUBSUB: Message published")
    publish(topic, payload)

    #Check when the file is uploaded

    resp = download_file(file_name)
    print("GCS: File Url ", resp)
    print("Spawning a thread to delete file from gcs...")
    t = threading.Thread(target=delete_file_from_storage_after_time_t, args=(15,file_name,)) 
    t.start()
    print("Thread started...")
    print("API: SUCCESS, Returning status ", 200)
    return resp, 200


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500

def delete_file_from_storage_after_time_t(t, file_name):
    time.sleep(t)
    delete_file_from_cloud(file_name)
    print("THREAD: Delete executed")

def find_available_storage(file_size):
    storage_id = None
    sd_collection = db.reference('/storageDevices')
    storage_dic = sd_collection.get()
    sorted_keys = sorted(storage_dic, key=lambda x: (storage_dic[x]['totalSpace']-storage_dic[x]['spaceUsed']))
    for k in sorted_keys:
        if((storage_dic[k]['totalSpace']-storage_dic[k]['spaceUsed'])>=file_size):
            storage_id = storage_dic[k]['storageId']
            storage_dic[k]['spaceUsed'] = storage_dic[k]['spaceUsed'] + file_size
            break
    sd_collection.set(storage_dic)
    return storage_id


def find_storage_id(user_id, file_name):
    print ('Finding File in Firebase!')
    file_name=file_name.replace('.','')
    print(file_name)
    path = ['users',user_id,'files',file_name,'storageId']
    path = "/".join(path)
    storage_id = db.reference(path).get()
    print("File found in storage id ", storage_id)
    return storage_id

def check_if_file_exists_das(user_id, file_name):
    path = ['users', user_id]
    path = "/".join(path)
    users_ref = db.reference(path)
    users_dic = users_ref.get()
    file_key = file_name.replace('.','')
    if 'files' in users_dic and file_key in users_dic['files']:
        return users_dic['files'][file_key]['storageId']
    return None

def update_user_quota(user_id, file_size, flag):  
    path = ['users', user_id]
    path = "/".join(path)
    users_ref = db.reference(path)
    users_dic = users_ref.get()
    print("FIREBASE: Userid ", user_id, " Current Space Used: ", users_dic['spaceUsed'])
    if flag:
        users_dic['spaceUsed'] = users_dic['spaceUsed'] + file_size
    else:
        users_dic['spaceUsed'] = users_dic['spaceUsed'] - file_size
    print("FIREBASE: UserId ", user_id, " Updated Space usage: ", users_dic['spaceUsed'])
    users_ref.set(users_dic)

def update_user_files(user_id,storage_id,file_name,file_size):
    file_key = file_name.replace('.','')
    file_object = {
        u'originalFileName':file_name ,
        u'fileName':str(user_id) + file_name, 
        u'fileSize':file_size,
        u'storageId':storage_id
    }
    db.reference("users").child(user_id).child('files').child(file_key).set(file_object)

def delete_user_file_DB(user_id,file_name):
    print("FIREBASE: Deleting file " , file_name, "from user ", user_id , " collection")
    path = ['users',user_id,'files']
    path = "/".join(path)
    users_files_ref = db.reference(path)
    files_dic = users_files_ref.get()
    file_key = file_name.replace('.','')
    file_size = files_dic[file_key]['fileSize']
    storage_id = files_dic[file_key]['storageId']
    del files_dic[file_key]
    users_files_ref.set(files_dic)
    print("FIREBASE: SUCCESS")
    
    print("FIREBASE: Updating available space for Storage Device: ", storage_id)
    
    path = ['storageDevices',storage_id]
    path = "/".join(path)
    sd_ref = db.reference(path)
    storage_dic = sd_ref.get()
    storage_dic['spaceUsed'] = storage_dic['spaceUsed'] - file_size
    sd_ref.set(storage_dic)
    print("FIREBASE: SUCCESS")
    
    return file_size
    

#Publish to a topic
# [START gae_flex_pubsub_index]
#@app.route('/publish', methods=['POST'])
def publish(topic, payload):
    print ('Publish started!')
    data = payload.encode('utf-8')
    #data = request.form.get('payload', 'Example payload').encode('utf-8')
    #topic = request.args.get('topic')
    topic_path = publisher.topic_path('cc2020project2', topic)
    publisher.publish(topic_path, data=data)
    resp = 'Message for topic ' + topic + ' published'
    return resp, 200
# [END gae_flex_pubsub_index]

def build_payload(action, file_name):
    d = {"action":action, "filename":file_name}
    return json.dumps(d)

#To upload file to Google Storage
def upload_file(file_obj, file_name):
    """Process the uploaded file and upload it to Google Cloud Storage."""
    print ('GCS: Uploading file to Google Cloud Storage!')
    uploaded_file = file_obj

    if not uploaded_file:
        return 'No file uploaded.', 400
        
    # Create a new blob and upload the file's content.
    blob = bucket.blob(file_name)

    blob.upload_from_string(
        uploaded_file.read(),
        content_type=uploaded_file.content_type
    )

    # The public URL can be used to directly access the uploaded file via HTTP.
    return blob.public_url


#To Delete file from Google Storage
#@app.route('/delete_blob', methods=['DELETE'])
def delete_file_from_cloud(blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    print ('GCS: Deletion from the Google Storage started!')
    blob = bucket.blob(blob_name)
    blob.delete()

    resp = "Blob {} from the Google Storage successfully deleted".format(blob_name)
    return resp

def check_file_in_cache(file_name):
    blob = bucket.blob(file_name)
    if blob.exists():
        return True
    else:
        return False

#To Delete file from Google Storage
#@app.route('/download_blob', methods=['GET'])
def download_file(file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"
    print ('GCS: Download started!')
    blob = bucket.blob(file_name)
    while not blob.exists():
        print ('GCS: Waiting for file to be available')

    #destination_file_name = file_name
    #blob.download_to_file_name(destination_file_name)

    #resp = file_name + ' downloaded as ' + destination_file_name + 'in your local directory'
    resp = 'https://storage.cloud.google.com/' + BUCKET_NAME + '/' + file_name
    return resp

#print (download_file('sample.png'))

if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
