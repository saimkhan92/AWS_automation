import json
import boto3
from botocore.client import ClientError
import os
import glob

from boto3.s3.transfer import S3Transfer



topic_arn=''
my_queue=''
pipe_id=''
def Main():
    global conn
    global transfer
    conn=boto3.client('s3')
    transfer=S3Transfer(conn)
    global local_conv_dir
    
    #Local directory where the transcoded videos will be downloaded
    local_conv_dir='E:\\transcode_out'
    global local_unconv_dir
    
    #Local directory from where the raw videos  will be pushed to the S3 bucket for transcoding
    local_unconv_dir='E:\\transcode_in'
    global file_path
    
    # Only .mp4 files will fetched and pushed to the S3 bucket
    file_path='*.mp4'
    new_files=set()
    global my_role
    
    # This creates a IAM role to which we will add permissions to access S3, SNS and SQS
    my_role='transcode_saim_iam'
    global s3
    s3=boto3.resource('s3')
    print(s3)
    for bucket in s3.buckets.all():
        print(bucket.name)
    global s3_in
    
    # This is the input bucket to which the files will be pushed
    s3_in='mytranscodeinsaim'
    global s3_out
    
    # This is the output bucket from which the files will be downloaded
    s3_out='mytranscodeoutsaim'
    global queue_name
    
    # We create a SQS queue to which the SNS messages will be pushed
    queue_name='transcode_sqssaim'
    
    global iam_policy
    global attach_policy
    
    # This is the policy document for the iam role
    iam_policy={
        "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
        "Service": "elastictranscoder.amazonaws.com"
        },
       "Action": "sts:AssumeRole"
       }
      ]
    }
    
    # We add this to the policy document to add permission to SQS, SNS and S3
    attach_policy={
  "Statement": [
    {
      "Action": [
        "sqs:*"
      ],
      "Effect": "Allow",
      "Resource": "*"
    },

    {
      "Effect": "Allow",
      "Action": [
         "s3:*"
        ],
      "Resource": "*"
    },
    {
      "Action": [
        "sns:*"
        ],
      "Effect": "Allow",
      "Resource": "*"
    },
  ]
}
    
    
    #This is the policy document to be attached to the SQS Queue
    global queue_policy
    queue_policy = {
        "Sid": "auto-transcode",
        "Effect": "Allow",
        "Principal": {
            "AWS": "*"
        },
        "Action": "SQS:SendMessage",
        "Resource": "<SQS QUEUE ARN>",
        "Condition": {
            "StringLike": {
                "aws:SourceArn": "<SNS TOPIC ARN>"
            }
        }
    }
    
    
    global pipeline_name
    
    #this is the name of the pipe that will be used to transcode pipe
    pipeline_name='transcode_pipesaim'
    #global topic_arn
    global my_role_new
    
    
    #Check for files that are already converted
    old_files=check_local_dir()
    #Check for all the settigs relevant to create a pipe
    check_aws(s3)
    
    while(True):
        
        temp=set(get_fileList())
        print(temp)
        print("Put your files in the local unconverted directory")
        new_files=temp.difference(old_files)
        
        if len(new_files)>0:
            for pp in new_files:
                file_name=upload_to_s3(pp)
            #topic_arn=create_sns_topic()
            #my_role_new=set_iam()
            #print('iam role............')
            #print(my_role_new)
            #my_queue=create_sqs()
            #pipe_id=create_pipeline(topic_arn,my_role_new)
            
                start_job(file_name)
            
        check_pipe()

# This is used to keep checking the pipe for messages.        
def check_pipe():
    down_list=check_queue()
    if down_list:
        for floo in down_list:
            download_file(floo)
            
# This method is used to download completed files to the local directory
def download_file(fin_file):
    destination_path = os.path.join(
            local_conv_dir,
            os.path.basename(fin_file)
        )
    print("Downloading converted file from S3 bucket")
    transfer.download_file(s3_out,fin_file,destination_path)
    
    
        
# This method will fetch all the messages in the queue and check for the SNS message sent after transcoding completed
def check_queue():
    file_list=[]
    #print("This is my_queue***")
    #print(my_queue)
    for msg in my_queue.receive_messages(WaitTimeSeconds=10):
        #print(type(msg))
        body=json.loads(msg.body)
        temp=body.get('Message','{}')
        curr=json.loads(temp).get('outputs','[]')
        if curr[0]['status']=='Complete':
            print("Status-Completed message found in Queue")
            com_file=curr[0]['key']
            file_list.append(com_file)
    return file_list
        
        
# This method will initialize a transcoding job using the pipe line that is created   
def start_job(filename):
    transcoder=boto3.client('elastictranscoder','us-west-2')
    transcoder.create_job(
            PipelineId=pipe_id,
            Input={
                'Key': filename,
                'FrameRate': 'auto',
                'Resolution': 'auto',
                'AspectRatio': 'auto',
                'Interlaced': 'auto',
                'Container': 'auto'
            },
            Outputs=[{
                'Key': '.'.join(filename.split('.')[:-1]) + '.mp4',
                'PresetId': '1351620000001-000001'                            # This preset id is used to convert the files to 1080p resolution
            }]
        )
    
    
    
#This method is used to create a sqs queue
def create_sqs():
    #sqs_1=boto3.client('sqs','us-west-2')
    sqs_2=boto3.resource('sqs','us-west-2')
    sns_1=boto3.resource('sns','us-west-2')
    queue=sqs_2.create_queue(QueueName=queue_name)
    #queue_url=queue.url
    queue_arn=queue.attributes['QueueArn']
    #print("Before call")
    #print(topic_arn)
    topics_arn=sns_1.Topic(topic_arn)
    sub_flag=False
    for subs in topics_arn.subscriptions.all():
            if subs.attributes['Endpoint']==queue_arn:
                sub_flag=True
                break
    #print('This is endpoint')
    #print(queue_arn)
    if not sub_flag:
        topics_arn.subscribe(Protocol='sqs',Endpoint=queue_arn)
        
    if 'Policy' in queue.attributes:
            policy = json.loads(queue.attributes['Policy'])
    else:
        policy = {'Version': '2008-10-17'}

    if 'Statement' not in policy:
        statement = queue_policy
        statement['Resource'] = queue_arn
        statement['Condition']['StringLike']['aws:SourceArn'] = \
                topic_arn
        policy['Statement'] = [statement]
        
        
        
        queue.set_attributes(Attributes={
            'Policy': json.dumps(policy)
        })

    return queue



# This method is used to setup iam role 
def set_iam():
    iam_obj=boto3.client('iam')
    role_dict=iam_obj.list_roles()
    role_list=role_dict['Roles']
    for obj in role_list:
        if obj['RoleName']==my_role:
            role_res=iam_obj.get_role(RoleName=my_role)
            #print("From present......")
            #print(role_res['Role']['Arn'])
            return role_res['Role']['Arn']
    role_res=create_iamRole(iam_obj)    
    return role_res['Role']['Arn']

def create_iamRole(iam):
    role_res=iam.create_role(
        RoleName=my_role,
        AssumeRolePolicyDocument=json.dumps(iam_policy)
        )
    iam.put_role_policy(
    RoleName=my_role,
    PolicyName='attach_policy',
    PolicyDocument=json.dumps(attach_policy))
    return role_res

#This method will create the SNS toopic
def create_sns_topic():
    sns=boto3.resource('sns','us-west-2')
    topic=sns.create_topic(Name='transcode-complete').arn 
    return topic        
            
#This method will create a elastic transcoder pipeline
def create_pipeline(topic_arn,my_role_res):
    transcoder=boto3.client('elastictranscoder','us-west-2')
    pipe_list=transcoder.list_pipelines()
    flag=False
    for pipes in pipe_list['Pipelines']:
        
        if pipes['Name']==pipeline_name:
            flag=True
            #print("Ye if wala")
            #print(pipes['Id'])
            return pipes['Id']
        
    if flag == False:        
        res=transcoder.create_pipeline(
                Name='transcodepipe',
                InputBucket=s3_in,
                OutputBucket=s3_out,
                Role=my_role_res,
                Notifications={
                    'Progressing': '',
                    'Completed': topic_arn,
                    'Warning': '',
                    'Error': ''
                })
        #print("Ye dusra if wala")
        #print(res['Pipeline']['Id'])
        return res['Pipeline']['Id'] 

class LocalSetupError(Exception):
    pass


# This method will upload files to the S3 bucket
def upload_to_s3(path):
   
    print(path)
    old_files.add(path)
    transfer.upload_file(path,s3_in,os.path.basename(path))
    print("Transfer Complete to S3 bucket")
    return os.path.basename(path)
    #sys.exit()
    
    
def check_aws(s3_obj):
    #print("******************")
    #print(s3_obj)
#    conn=boto3.client('s3')
#    transfer=S3Transfer(conn)
    if not s3_bucket_exists(s3_in,s3_obj):
        conn.create_bucket(Bucket='mytranscodeinsaim')
        
        
        
    if not s3_bucket_exists(s3_out,s3_obj):
        
        conn.create_bucket(Bucket='mytranscodeoutsaim')
    global topic_arn                                                                        
    topic_arn=create_sns_topic()
    #print("This is arn")
    #print(topic_arn)
    my_role_new=set_iam()
    #print('iam role............')
    #print(my_role_new)
    global my_queue
    my_queue=create_sqs()
    #print("Typeeeeee")
    #print(type(my_queue))
    global pipe_id
    pipe_id=create_pipeline(topic_arn,my_role_new)   
           
    
        #sys.exit()
        
# To check and create  S3 buckets
def s3_bucket_exists(in_name,s3_temp):
    
        if s3_temp.Bucket(in_name) in s3_temp.buckets.all():
            print("Bucket exists")
            return True
        else:
            print("Bucket doesn't exist")
            return False
        
#Poll the folders  to check for new files

def check_local_dir():
    if local_conv_dir==local_unconv_dir:
        raise LocalSetupError('The converted and unconverted directories cannot be same')
    if not os.path.exists(local_conv_dir):
        #print('iff1')
        os.makedirs(local_conv_dir)
    if not os.path.exists(local_unconv_dir):
        #print('iff2')
        os.makedirs(local_unconv_dir)
    else:    
        global old_files
        old_files=set(get_fileList())
        #print("List")
        #print(old_files)
    
    print("Local Directory checked")    
    return old_files

def get_fileList():
    path=os.path.join(local_unconv_dir,file_path)
    return glob.glob(path)    
    
    
if __name__=="__main__":
    Main()