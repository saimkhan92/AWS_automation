import boto3
import os
import time
from boto3.s3.transfer import TransferConfig, S3Transfer
                 
def exit_main():
    print("\n"+"Do you want to Continue?"+"\n")
    exit_choice=int(input("Press 1 for YES and 2 for NO"+"\n"))
    if(exit_choice==1):
        b=0
        return b
    else:
        b=1
        return b
s3=boto3.resource('s3')

#s3.create_bucket(Bucket="Bucket")

global a
a=0
while a==0:
    print("These are the Buckets that are currently available:")
    for bucket in s3.buckets.all():
        print("->  "+bucket.name+"\n")
    print("Do you want to create a new bucket or use an existing one?\n")
    print("1. Create a NEW Bucket."+"\n")
    print("2. Use an EXISTING Bucket."+"\n")
    user_selection=int(input("Please make a Selection."))
    if(user_selection==1):
        print("\n"+"\t\t"+"****************  You chose to create a NEW Bucket  ***************"+"\n")
        print("\n")
        name_selection_1=str(input("Please enter the name of the Bucket you wish to create. VALID bucket names only please."))
        connection=boto3.client('s3')
        connection.create_bucket(Bucket=name_selection_1)
        file_path=str(input("Enter the PATH of the file that you would want to push into the bucket."))
                
        files = [
            file_path
        ]
        
        s3resource = boto3.resource('s3')
        s3client = s3resource.meta.client
        
        
        for path in files:
            print(path)
            file = os.path.basename(path)
            print("*** uploading "+file)
            start = time.time()
            configuration=TransferConfig(multipart_threshold=8*1024*1024,max_concurrency=10,num_download_attempts=10)
            client=boto3.client('s3')
            transfer=S3Transfer(client,configuration)
            s3client.upload_file(path, name_selection_1, file)
            end = time.time()
            print('time elapsed: '+str(end - start))
        exit_main()
    elif(user_selection==2):
        print("\n"+"\t\t"+"****************  You chose to use an EXISTING Bucket  ***************"+"\n")
        print("\n")
        name_selection_2=str(input("Enter the name of the bucket you would like to access."))
        connection=boto3.client('s3')
        connection.create_bucket(Bucket=name_selection_2)
        print("\n"+"Files that are currently inside this bucket:"+"\n")
        for key in connection.list_objects(Bucket=name_selection_2)['Contents']:
            print(key['Key'])
        selection=(int(input("Press 1 if you want to push a file into this bucket OR Press 2 if you wish to download a file from this bucket.")))
        if(selection==1):
        
            file_path=str(input("Enter the PATH of the file that you would want to push into the bucket."))
                    
            files = [
                file_path
            ]
            
            s3resource = boto3.resource('s3')
            s3client = s3resource.meta.client
            
            
            for path in files:
                print(path)
                file = os.path.basename(path)
                print("*** uploading "+file)
                start = time.time()
                configuration=TransferConfig(multipart_threshold=8*1024*1024,max_concurrency=10,num_download_attempts=10)
                client=boto3.client('s3')
                transfer=S3Transfer(client,configuration)
                s3client.upload_file(path, name_selection_2, file)
                end = time.time()
                print('time elapsed: '+str(end - start))
        else:
            configuration=TransferConfig(multipart_threshold=8*1024*1024,max_concurrency=10,num_download_attempts=10)
            client=boto3.client('s3')
            transfer=S3Transfer(client,configuration)
            name=input("Enter a file name from the list that you would like to Download")
            path=input("Please Specify the local destination path")
            transfer.download_file(name_selection_2, name, path)
            
        
        a=exit_main()
              
print("Thank You. The Program will now EXIT.")