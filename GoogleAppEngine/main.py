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
storage_client = storage.Client()
bucket = storage_client.bucket('cc2020project2_bucket')
URL_CACHE = 'https://storage.cloud.google.com/' + BUCKET_NAME + '/' 


def test():
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    acl = bucket.acl
    acl.user("apoorv2arsenalfc@gmail.com").grant_read()


@app.route('/', methods=['GET'])
def index():
    return "App running", 200
    
@cross_origin(supports_credentials=True)
@app.route('/upload', methods=['POST'])
def upload():
    #print (request.data)
    print ('Upload Started!')
    body = request.get_json()
    userid = request.form.get('userId')
    file_obj = request.files.get('file')
    print (type(file_obj))
    file_obj_read=file_obj.read()
    file_obj.seek(0)
    file_size = len(file_obj_read)
    print("File size is", file_size)

    filename = file_obj.filename
    originalfilename = filename

    filename = str(userid) + filename 
    print (filename)

    url=upload_file(file_obj, filename)
    # file_size = 10
    # Find available storage, update its space in DB
    storage_id = find_available_storage(file_size)

    topic = storage_id
    print(topic)
    # Update users collections
    
    update_user_files(userid,storage_id,originalfilename,file_size)
    print("Done3")
    action = "download"
    print("Done4")
    payload = build_payload(action,filename)
    print("Done5")
    publish(topic, payload)
    print ('Publish done!')
    return 'UPLOAD_COMPLETE', 200
    
    #filename= secure_filename(f.filename)

    

#TODO
@cross_origin(supports_credentials=True)
@app.route('/delete', methods=['POST'])
def delete():
    print ('Deletion Started!')
    body = request.get_json()
    print (body)
    userid = body['userId']
    filename = body['file']
    print (userid, filename)
    originalfilename = filename
    filename = str(userid) + filename 

    #TODO DB Find on which device user's file is stored
    storage_id = find_storage_id(userid, originalfilename)  
    topic = storage_id
    action = "delete"
    payload = build_payload(action, filename)
    publish(topic, payload)
    #Updating Delete DATABASE
    print("Updating Firebase for deleting the file")
    delete_user_file_DB(userid, originalfilename)

    return 'DELETE_COMPLETE', 200

#TODO
@cross_origin(supports_credentials=True)
@app.route('/download', methods=['POST'])
def download():
    print ('Download Started')

    body = request.get_json()
    print (body)
    userid = body['userId']
    filename = body['file']
    print (userid, filename)
    originalfilename = filename
    filename = str(userid) + filename 
    

    if check_file_in_cache(filename):
        return URL_CACHE+filename, 200
    

    #TODO DB Operation find the storage id containing user file
    storage_id = find_storage_id(userid, originalfilename)
    print(storage_id)
    if storage_id==None:
        return 'No such file Exists!'
    topic = storage_id
    print (topic)
    action = "upload"
   
    payload = build_payload(action,filename)
    publish(topic, payload)

    #Check when the file is uploaded

    resp = download_file(filename)
    print (resp)
    t = threading.Thread(target=delete_file_from_storage_after_time_t, args=(15,filename,)) 
    t.start()
    #print (delete_file_from_cloud(filename))
    return resp
    #TODO Return the File to UI
    # Create path of the file, this method will return file as attachment to UI
    #path = os.path.join(os.getcwd(),"filename")
    #return send_file(path, as_attachment=True)



@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500

def delete_file_from_storage_after_time_t(t, filename):
    time.sleep(t)
    delete_file_from_cloud(filename)

def find_available_storage(file_size):
    storageid = None
    sd_collection = db.reference('/storageDevices')
    storage_dic = sd_collection.get()
    for k,device_obj in storage_dic.items():
        print(device_obj)
        if(device_obj['availableSpace']>=file_size):
            storageid = device_obj['storageId']
            print (storageid)
            storage_dic[k]['availableSpace'] = storage_dic[k]['availableSpace']-file_size
            break
    sd_collection.set(storage_dic)
    return storageid


def find_storage_id(userid, filename):
    print ('Finding File in Firebase!')
    filename=filename.replace('.','')
    print(filename)
    path = ['users',userid,'files',filename,'storageId']
    path = "/".join(path)
    storageid = db.reference(path).get()
    print(type(storageid))
    print("asdasd",storageid)
    return storageid

def update_user_files(userid,storageid,filename,filesize):
    print('Updating user file in Firebase!')
    file_key = filename.replace('.','')
    file_object = {
        u'originalFileName':filename ,
        u'fileName':str(userid) + filename, 
        u'fileSize':filesize,
        u'storageId':storageid
    }
    db.reference("users").child(userid).child('files').child(file_key).set(file_object)

def delete_user_file_DB(userid,filename):
    path = ['users',userid,'files']
    path = "/".join(path)
    users_files_ref = db.reference(path)
    files_dic = users_files_ref.get()
    file_key = filename.replace('.','')
    filesize = files_dic[file_key]['fileSize']
    storageid = files_dic[file_key]['storageId']
    del files_dic[file_key]
    users_files_ref.set(files_dic)
    
    path = ['storageDevices',storageid]
    path = "/".join(path)
    sd_ref = db.reference(path)
    storage_dic = sd_ref.get()
    print(storage_dic)
    storage_dic['availableSpace']=storage_dic['availableSpace']+filesize
    sd_ref.set(storage_dic)
    print("Updated Storage Device and User Files")

    

#Publish to a topic
# [START gae_flex_pubsub_index]
#@app.route('/publish', methods=['POST'])
def publish(topic, payload):
    print ('Publish started!')
    print (topic)
    print(type(payload))

    data = payload.encode('utf-8')
    print("Hello")
    #data = request.form.get('payload', 'Example payload').encode('utf-8')
    #topic = request.args.get('topic')
    topic_path = publisher.topic_path('cc2020project2', topic)
    print (topic_path)
    publisher.publish(topic_path, data=data)
    resp = 'Message for topic ' + topic + ' published'
    return resp, 200
# [END gae_flex_pubsub_index]

def build_payload(action, filename):
    #print(action)
    #print(filename)
    d = {"action":action, "filename":filename}
    return json.dumps(d)

#To upload file to Google Storage
def upload_file(file_obj, filename):
    """Process the uploaded file and upload it to Google Cloud Storage."""
    print ('Uploading file to Google Cloud Storage!')
    uploaded_file = file_obj

    if not uploaded_file:
        return 'No file uploaded.', 400

    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.get_bucket('cc2020project2_bucket')

    # Create a new blob and upload the file's content.
    blob = bucket.blob(filename)

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
    print ('Deletion from the Google Storage started!')
    storage_client = storage.Client()
    bucket = storage_client.bucket('cc2020project2_bucket')
    blob = bucket.blob(blob_name)
    blob.delete()

    resp = "Blob {} from the Google Storage successfully deleted".format(blob_name)
    return resp

def check_file_in_cache(filename):
    blob = bucket.blob(filename)
    #print (blob.exists())
    if blob.exists():
        return True
    else:
        return False

#To Delete file from Google Storage
#@app.route('/download_blob', methods=['GET'])
def download_file(filename):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"
    print ('Download started!')
    storage_client = storage.Client()
    bucket = storage_client.bucket('cc2020project2_bucket')
    blob = bucket.blob(filename)
    #print (blob.exists())
    while not blob.exists():
        print ('Waiting for file to be available!')

    #destination_file_name = filename
    #blob.download_to_filename(destination_file_name)

    #resp = filename + ' downloaded as ' + destination_file_name + 'in your local directory'
    resp = 'https://storage.cloud.google.com/' + BUCKET_NAME + '/' + filename
    return resp

#print (download_file('sample.png'))

if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=3000, debug=True)
