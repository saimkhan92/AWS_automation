import datetime
import boto.ec2.cloudwatch
import boto.ec2
import boto.sns
import logging
import boto.sqs
import matplotlib.pyplot as plt
import time

ec2_conn = boto.ec2.connect_to_region('us-west-1')
print("Successfully established connection to: ", ec2_conn)
c = boto.ec2.cloudwatch.connect_to_region('us-west-1')
print("Successfully established connection to: ", c)
logging.basicConfig(filename="sns-topic.log", level= logging.DEBUG)
conn = boto.sns.connect_to_region("us-west-1")
print("Connection successfully established to: ", conn)

def Mail(arn):
    
    t2 = conn.get_all_subscriptions_by_topic(arn)
    s = []
    for item in t2['ListSubscriptionsByTopicResponse']['ListSubscriptionsByTopicResult']['Subscriptions']:
        if str(item['TopicArn']) == str(arn):
            s = item['SubscriptionArn']
    a = 0
    t_end = time.time() + 60  
     
    if str(s) == 'PendingConfirmation':
        while time.time()<t_end:
            print("Waiting for Confirmation")
            t2 = conn.get_all_subscriptions_by_topic(arn)
            for item in t2['ListSubscriptionsByTopicResponse']['ListSubscriptionsByTopicResult']['Subscriptions']:
                if str(item['TopicArn']) == str(arn):
                    s = item['SubscriptionArn']
            if str(s) != "PendingConfirmation":
                print("Subscription Confirmed")   
                message = "Dear User,\nYou have exceeded the acceptable threshold for CPU Utilization.Please close unnecessary applications to reduce CPU processing."
                sub = "AWS SNS TEST - CPUUtilization Alert"
                pub=conn.publish(arn,message,subject=sub)
                print ("Mail sent") 
                #quit()
        print("Invalid Email\nPlease try again")
    else:
        message = "Dear User,\nYou have exceeded the acceptable threshold for CPU Utilization.Please close unnecessary applications to reduce CPU processing."
        sub = "AWS SNS TEST - CPUUtilization Alert"
        pub=conn.publish(arn,message,subject=sub)
        print ("Mail sent") 
        #quit()   
        
def Subscription():    
    t = conn.get_all_topics()
    t1 = []
    topics = []
    for item in t['ListTopicsResponse']['ListTopicsResult']['Topics']:
        t1.append(item['TopicArn'])
        topics.append(item['TopicArn'].split(':')[5])
    print("Available Topics",topics)
    check = []    
    i =0
    l = len(topics)
    a = 0
    topic = input("Please enter a new topic name")
    while i<l:
        if topic == topics[i]:
            check.append(topics[i])
        i = i+1   
    if len(check) >0:
        print("Topic Already exists")
        email = input("Please enter a valid email id")
        index = topics.index(topic)
        arn = t1[index] 
        subs = conn.subscribe(arn,'email',email)
        print("Successfully subscribed to Topic: ",arn) 
        Mail(arn)
        #i=0
        #check=[]
    else:
        
        topicarn = conn.create_topic(topic)
        print("Created topic ",topic)
        arn = topicarn['CreateTopicResponse']['CreateTopicResult']['TopicArn']
        email = input("Please enter your email ID")
        subs = conn.subscribe(arn,'email',email)
        print("Successfully subscribed to Topic: ",arn) 
        Mail(arn)
        
def CPUUtilization (start,end,id):
    data2 = c.get_metric_statistics(300,start,end,'CPUUtilization', 'AWS/EC2',statistics = 'Average', 
                        dimensions = {'InstanceId' : [id]})
    percent =[]
    for item in data2:
        percent.append(item['Average'])
    return percent
    

try:
    reservations = ec2_conn.get_all_reservations()
    instanceid = reservations[0].instances[0]
    instances = ec2_conn.get_all_instances()
    instance_names = []
    instance_id = []
    instance_state = []
    for r in reservations:
        for i in r.instances:
            if 'Name' in i.tags:
                instance_names.append(i.tags['Name'])
                instance_id.append(i.id)
                instance_state.append(i.state)
            else:
                print (i.id, i.state)
    end = datetime.datetime.utcnow()
    start = datetime.datetime.utcnow()-datetime.timedelta(hours = 2)
    i = 0
    l = len(instance_names)
    print("Your available instances are:")
    while i < l:
        print(instance_names[i])
        i = i+1
    i_name = input("Enter an instance name you would like to monitor")
    if i_name in instance_names:
        index = instance_names.index(i_name)
        percent = CPUUtilization(start, end, instance_id[index])
        print(percent)
        s = len(percent)
        j = 0
        k = 0
        x = []
        while k<s :
            x.append(j)
            j = j+ 5
            k = k+1
        if len(x) == 1:
            print("Insufficient Data")
        else:
            plt.plot(x,percent)
            plt.xlabel("Time(minutes")
            plt.ylabel("CPU Utilization (Percentage)")
            print("View your CPU Utilization Metrics")
            plt.show()
        threshold = 0.034
        for i in percent:
            if i>threshold:
                print("Your Usage has exceeded the limit: ", i) 
                Subscription()
                break
    else:
        print("Invalid Instance name")

except:
    print("Error")
    
quit()
  
