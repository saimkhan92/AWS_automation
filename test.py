import boto3
from idlelib.EditorWindow import keynames
ec2 = boto3.resource("ec2")

def show_running_instances():
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    for instance in instances:
        print(instance.id, instance.instance_type)

def show_instance_status():   
    for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
        print(type(status))
        print("INSTANCE ID"+"     "+"AVAILABILITY ZONE"+"     "+"STATUS"+"     "+"SYSTEM CHECKS(OK or IMPAIRED)"+"\n")
        print(status["InstanceId"]+"         "+status['AvailabilityZone']+"         "+status['InstanceState']["Name"]+"         "+status['SystemStatus']['Status']+"\n")

def show_stopped_instances():
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}])
    for instance in instances:
        print(instance.id, instance.instance_type)

def create_instance():
    print("Welcome to the instance creation wizard /n")
    choice=int(input("Please select from the following three choices.\n 1)Linux EC2 instance with all services enabled (Not Recommended) \n 2) Database Server \n3) Web Server \n4) Custom Selection "))
    if choice==1:
        ec2.create_instances(ImageId="ami-c229c0a2", MinCount=1, MaxCount=1, SecurityGroupIds=['sg-c23a7ea5'],InstanceType='t1.micro')
    elif choice==2:
        ec2.create_instances(ImageId="ami-c229c0a2", MinCount=1, MaxCount=1, SecurityGroupIds=['sg-cdd89caa'],InstanceType='t1.micro')
    elif choice==3:
        ec2.create_instances(ImageId="ami-c229c0a2", MinCount=1, MaxCount=1, SecurityGroupIds=['sg-e0d89c87'],InstanceType='t1.micro')
    elif choice==4:
        print("You have chosen the custom instance creation option")
        var_security_group=input("Specify the security group for this machine 1,2,3 or 4\n 1) Open(sg-c23a7ea5) 2) Database Security settings(sg-cdd89caa)  3) Web server security settings(sg-e0d89c87)")
        #if not already present, create new 
        var_monitoring=bool(input("Do you want to enable system monitoring \n 1) True   2) False "))
        var_api_termination=bool(input("Do you want to disable Api Terminate? \n 1) True   2) False "))
        var_key_name=input("Enter the name of the key pair you want to use. (ubuntu_key_pair) Type create_new if you want to create a new key pair.")
        if var_key_name=="create_new":
            name=input("Enter the name of the key you want to create")
            create_key_pair(name)
        var_instance_type=input("Which type of instance do you want? instance type: 1)general purpose(t2.micro) 2)compute optimized(c3.large), 3)memory optimized(r3.large), 4)storage optimized(d2.large), 5)GPU instances(g2.2xlarge)")
        number_of_instances=int(input("How many instances do you want to create?"))
        ec2.create_instances(ImageId="ami-c229c0a2", MinCount=number_of_instances, MaxCount=number_of_instances, SecurityGroupIds=[var_security_group],InstanceType=var_instance_type,KeyName=var_key_name,    DisableApiTermination=var_api_termination,Monitoring={'Enabled': var_monitoring})
    #ec2.create_instances(ImageId=image_id_choice, MinCount=1, MaxCount=2, SecurityGroupIds=['sg-e5a4de82'])

def terminate_instance():
    instance_list=[]
    print("The following is the list of stopped instances")
    show_stopped_instances()
    while True:
        instance=input("\nInput the name of the instance to terminate OR type exit")
        if instance=="exit":
            break
        instance_list.append(instance)
        
    ec2.instances.terminate(DryRun=False,InstanceIds=instance_list)

def create_key_pair(key_name):
    key_pair = ec2.create_key_pair(KeyName=key_name)
    key_string=key_pair.key_material
    fh=open(key_name+".pem","w")
    fh.write(key_string)
    
    
    
    
#create_key_pair("saimkey")
#terminate_instance()    
#show_stopped_instances()
#create_instance("ami-1719f677")
#create_instance("ami-08111162")
#create_instance()
#show_instance_status()
