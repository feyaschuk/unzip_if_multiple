import os
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from zipfile import ZipFile
import shutil

token_credential = ClientSecretCredential()
     
OAUTH_STORAGE_ACCOUNT_NAME = "prodeastus2data"
oauth_url = f"https://{OAUTH_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

blob_service_client = BlobServiceClient(account_url=oauth_url, credential=token_credential)
dr_dir = blob_service_client.get_container_client("")
folder_to_save = os.getcwd() + '\\'+'sasb'


def unzip(file_to_extract, folder_to_save):    
    with ZipFile(file_to_extract, 'r') as zipObject:
        listOfFileNames = zipObject.namelist()
        if any(fileName.endswith('.csv') for fileName in listOfFileNames):    
            for fileName in listOfFileNames:
                if fileName.endswith('.csv') and fileName not in folder_to_save:                         
                    zipObject.extract(fileName, folder_to_save)
                    print('Csv file is extracted.')               
            for filename in os.listdir(folder_to_save):
                basename, extension = os.path.splitext(filename)     
                system_name, feed1, feed2, some_date, version = basename.split('_') 
                if len(some_date) < 8:
                    some_date = '20'+ some_date    
                    correct_filename = ("{}_{}_{}_{}_{}{}".format(system_name, feed1, feed2, some_date, version, extension))    
                    try:
                        os.rename(folder_to_save + '\\' + filename, folder_to_save + '\\' + correct_filename)
                    except FileExistsError:
                        print("File already exists.")
                    return system_name, correct_filename
        else:
            print('There is no csv file in the folder.')
                    

def save_blobs(filter_date=""):        
    for blob in dr_dir.list_blobs(name_starts_with="raw/PL_SIC_"):                  
        blob_name = blob.name 
        filename_date = ''.join(filter(lambda i: i.isdigit(), blob_name))  
        blob = blob_service_client.get_blob_client("", blob_name)      
        if filter_date in filename_date: 
            blob_content = blob.download_blob().readall() 
            file_to_extract = blob_name.split('/').pop()            
            with open(file_to_extract, "wb+") as blob_file:
                blob_file.write(blob_content)            
            return file_to_extract
        else:
            print("There is no file with this date. Check if the date is correct.")
    else:
        print("There is no file with name starts with raw/PL_SIC_.")         


try:
            
    file_to_extract = save_blobs(filter_date="20221129")    
    system_name, correct_filename = unzip(file_to_extract, folder_to_save)        
    blob_client = blob_service_client.get_blob_client("", 
    blob ='dropdir/sasb/' + system_name.lower() + '\\' + correct_filename)         
    with open(folder_to_save + '\\' + correct_filename, 'rb') as f:
        blob_client.upload_blob(f)    
        shutil.rmtree(folder_to_save)
        os.remove(file_to_extract) 
        print('File uploaded successfully to /dropdir/sasb/' 
        + system_name.lower())
            
except Exception as ex:
    if os.path.isdir(folder_to_save):
        shutil.rmtree(folder_to_save)
    if os.path.isfile(file_to_extract):
        os.remove(file_to_extract) 
    print('Exception:')
    print(ex)

