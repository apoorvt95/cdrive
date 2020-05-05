from google.cloud import pubsub_v1
import json
import os
from google.cloud import storage
import threading 
import magic
#importing module 
import time
import logging 
  
#Create and configure logger 
logging.basicConfig(filename="newfile.log", 
                    format='%(asctime)s %(message)s', 
                    filemode='w') 
  
#Creating an object 
logger=logging.getLogger() 
  
#Setting the threshold of logger to DEBUG 
logger.setLevel(logging.DEBUG) 
  
#Test messages 
'''
logger.debug("Harmless debug Message") 
logger.info("Just an information") 
logger.warning("Its a Warning") 
logger.error("Did you try to divide by zero") 
logger.critical("Internet is down") 
'''
# TODO project_id = "Your Google Cloud Project ID"
# TODO subscription_name = "Your Pub/Sub subscription name"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="cc2020project2-17090316f23c.json"
subscriber = pubsub_v1.SubscriberClient()
project_id = 'cc2020project2'
subscription_name = 'DAS1'
subscription_path = subscriber.subscription_path(
    project_id, subscription_name
)

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

def download_file(filename):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"
    print ('Download started!')
    #blob_name = request.args.get('filename')
    storage_client = storage.Client()

    bucket = storage_client.bucket('cc2020project2_bucket')
    source_blob_name = filename
    blob = bucket.blob(source_blob_name)
    destination_file_name = filename
    blob.download_to_filename(destination_file_name)

    resp = source_blob_name + ' downloaded as ' + destination_file_name
    print(resp)

    resp = delete_file_from_cloud(filename)
    print(resp)

def check_file_exist(filename):
    f = open(filename, "rb")

    if not f:
        print("Not file")
        f.close()
        return False
    else:
        print("File opened")
        f.close()
        return True
    

#To upload file to Google Storage
def upload_file(filename):
    try:
        """Process the uploaded file and upload it to Google Cloud Storage."""
        if(not check_file_exist(filename)):
            logger.error("UPLOAD - File doesn't exist")
            return
        
        f = open(filename, "rb")
        upload_file = f

        # Create a Cloud Storage client.
        gcs = storage.Client()

        # Get the bucket that the file will be uploaded to.
        bucket = gcs.get_bucket('cc2020project2_bucket')

        # Create a new blob and upload the file's content.
        blob = bucket.blob(filename)
        mime = magic.Magic(mime=True)
        content_type =mime.from_file(filename)
        blob.upload_from_string(
            upload_file.read(),
            content_type=content_type
        )
        print (blob.public_url)
        # The public URL can be used to directly access the uploaded file via HTTP.
        print( 'File uploaded succesfully in Google Storage at ' + blob.public_url)
    except IOError:
        print("File not accessible")
#upload_file('1sample.png')
#Delete file locally
def delete_file(filename):
    try:
        print(filename)
        while True:
            time.sleep(15)
            if(not check_file_exist(filename)):
                logger.info("DELETE: Trying to delete file " + filename)
                continue
            os.remove(filename)
            logger.info("DELETE: Deleted file " + filename)
            break        
    except IOError:
        print("File not accessible")
   
def callback(message):

    print("Received message: {}".format(message))

    json_object = json.loads(message.data.decode(encoding='utf-8'))
    action = json_object['action'] 
    filename = json_object['filename']
    if action == 'download':
        t = threading.Thread(target=download_file, args=(filename,)) 
        t.start()
        #download_file(filename)
    elif action == 'delete':
        t = threading.Thread(target=delete_file, args=(filename,)) 
        t.start()
    elif action == 'upload':
        t = threading.Thread(target=upload_file, args=(filename,)) 
        t.start()

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
while True:
    #time.sleep(0.1)
    streaming_pull_future.result()

subscriber.close()
