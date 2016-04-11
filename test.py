# Program to automate various functions of AWS EC2 including instance creation, key-pair generation, instance termination, status check etc.

import boto3
ec2 = boto3.resource("ec2")

def show_running_instances():
    print("\n")
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    for instance in instances:
        print(instance.id, instance.instance_type)
        print("\n")

def show_instance_status():
    print("\n")
    for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
        #print(type(status))
        print("INSTANCE ID"+"     "+"AVAILABILITY ZONE"+"     "+"STATUS"+"     "+"SYSTEM CHECKS(OK or IMPAIRED)"+"\n")
        print(status["InstanceId"]+"         "+status['AvailabilityZone']+"         "+status['InstanceState']["Name"]+"         "+status['SystemStatus']['Status']+"\n")
        print("\n")
        
def show_stopped_instances():
    print("\n")
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}])
    for instance in instances:
        print(instance.id, instance.instance_type)
    print("\n")

def create_instance():
    print("\n")
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
        var_monitoring=bool(input("Do you want to enable system monitoring \n 1) True   2) False "))
        var_api_termination=bool(input("Do you want to disable Api Terminate? \n 1) True   2) False "))
        var_key_name=input("Enter the name of the key pair you want to use. (ubuntu_key_pair) Type create_new if you want to create a new key pair.")
        if var_key_name=="create_new":
            name=input("Enter the name of the key you want to create")
            create_key_pair(name)
            print("The private key has been saved in your desktop")
        var_instance_type=input("Which type of instance do you want? instance type: 1)general purpose(t2.micro) 2)compute optimized(c3.large), 3)memory optimized(r3.large), 4)storage optimized(d2.large), 5)GPU instances(g2.2xlarge)")
        number_of_instances=int(input("How many instances do you want to create?"))
        ec2.create_instances(ImageId="ami-c229c0a2", MinCount=number_of_instances, MaxCount=number_of_instances, SecurityGroupIds=[var_security_group],InstanceType=var_instance_type,KeyName=var_key_name,DisableApiTermination=var_api_termination,Monitoring={'Enabled': var_monitoring})
        print("Instances have been created\n")
        
def terminate_instance():
    print("\n")
    instance_list=[]
    print("The following is the list of stopped instances")
    show_stopped_instances()
    while True:
        instance=input("\nInput the ID of the instance you terminate. You can enter multiple instances. Type <exit> when you are done")
        if instance=="exit":
            break
        instance_list.append(instance)
    try:
        ec2.instances.terminate(DryRun=False,InstanceIds=instance_list)
        print("Instances successfully terminated")
    except:
        print("Either you do not have the permission to terminate this instance or you entered incorrect instance name")
    print("\n")

def create_key_pair(key_name):
    print("\n")
    key_pair = ec2.create_key_pair(KeyName=key_name)
    key_string=key_pair.key_material
    complete_path="C:\\Users\\Saim\\Desktop\\"+key_name+".pem"
    fh=open(complete_path,"w")
    fh.write(key_string)
    print("Private key has been saved to your desktop\n")  
      
def main():
    while True:
        
        choice=input("\nPlease select an option from the following:\n 1) Show Running Instances\n 2) Show status running instances\n 3) Show stopped instances\n 4) Enter instance creation wizard\n 5) Terminate an instance\n 6) Create and download private key\n 7) Exit\n")
        if choice=="1":
            show_running_instances()
            continue
        elif choice=="2":
            show_instance_status()
            continue
        elif choice=="3":
            show_stopped_instances()
            continue
        elif choice=="4":
            create_instance()
            continue
        elif choice=="5":
            terminate_instance()
            continue
        elif choice=="6":
            while True:
                try:    
                    key_name=input("Enter the name of the key-pair")
                    break
                except:
                    print("Please try again")
                    continue
            create_key_pair(key_name)
        elif choice=="7":
            print("\nEnd of the program\n")
            raise SystemExit
        else:
            print("Invalid choice. Please try again.")
            continue

if __name__=="__main__":
    main()
#create_key_pair("saim0key")
#terminate_instance()    
#show_stopped_instances()
#create_instance("ami-1719f677")
#create_instance("ami-08111162")
#create_instance()
#show_instance_status()
