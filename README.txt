--------------------------------------------------------------------------------------------
- CMB (Comcast Message Bus) README
--------------------------------------------------------------------------------------------

A highly available, horizontally scalable queuing and notification service compatible with 
AWS SQS and SNS. This document covers these topics:

- Quick Tutorial
- Installation Guide
- Monitoring, Logging
- Build CMB from Source
- Known Limitations

--------------------------------------------------------------------------------------------
- Quick Tutorial
--------------------------------------------------------------------------------------------

CMB consists of two separate services, CQS and CNS. CQS offers queuing services while CNS 
offers publish / subscribe notification services. Both services are API-compatible with 
Amazon Web Services SNS (Simple Notification Service) and SQS (Simple Queuing Service). CMB 
services are implemented with a Cassandra / Redis backend and are designed with high 
availability and horizontal scalability in mind.

For a detailed documentation of the CNS / CQS APIs please refer to the Amazon SNS / SQS 
specifications here:

http://docs.amazonwebservices.com/sns/latest/api/Welcome.html
http://docs.amazonwebservices.com/AWSSimpleQueueService/latest/APIReference/Welcome.html

Accessing CNS / CQS:

There are three different ways to access CNS / CQS services:

1. Using the web based Admin UI:

The Admin UI is a simple Web UI for testing and administration purposes. To access the 
Admin UI use any web browser and go to

CNS Admin URL: http://<cnshost>:<cnsport>/ADMIN
CQS Admin URL: http://<cqshost>:<cqsport>/ADMIN

2. Using the AWS SDK for Java or similar language bindings:

Amazon offers a Java SDK to access SNS / SQS and other AWS services. Since CNS / CQS are 
API compatible you can use the AWS SDK to access our implementation in the same way. Thus, 
instead of having to engineer your own REST requests you can get started with a few lines 
of simple code as the following example illustrates.

  String userId = "<user_id>";
  BasicAWSCredentials credentialsUser = new BasicAWSCredentials("<access_key>", "<secret_key>");
 
  String cqsServerUrl = "http://<cqs_host>:<cqs_port>/";
             
  AmazonSQS sqs = new AmazonSQSClient(credentialsUser);
  sqs.setEndpoint(cqsServerUrl);
 
  Random randomGenerator = new Random();
  String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
        
  HashMap<String, String> attributeParams = new HashMap<String, String>();
  CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
  createQueueRequest.setAttributes(attributeParams);
  String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

Amazon offers a few other language bindings of its SDK and there are also a number of third 
party SDKs available for languages not supported by Amazon.

3. Sending REST requests directly to the service endpoint:

All CNS / CQS features can also be accessed by sending REST requests as HTTP GET or POST 
directly to the service endpoints. Note that you need to timestamp and digitally sign 
every request if signature verification is enabled in cmb.proerties.

Example REST request to create a CQS queue using curl:

curl -d "Action=CreateQueue&SignatureMethod=HmacSHA256&AWSAccessKeyId=48JT2LKD3TX9X5JD6NMM&QueueName=TSTQ_-3098387640939725337&SignatureVersion=2&Version=2011-10-01&Signature=FemvuycfOczDIySdw9K4fjHvBWDm9W4iLDFUNQK220M%3D&Timestamp=2012-08-04T00%3A14%3A54.157Z" http://<cqs_host>:<cqs_port>

Example response:

<CreateQueueResponse>
    <CreateQueueResult>
        <QueueUrl>http://<cqs_host>:<cqs_port>/342126204596/TSTQ_-3098387640939725337</QueueUrl>
    </CreateQueueResult>
    <ResponseMetadata>
        <RequestId>ad0ea46c-23fb-49bf-bb79-3784140451ae</RequestId>
    </ResponseMetadata>
</CreateQueueResponse> 

--------------------------------------------------------------------------------------------
- Installation Guide
--------------------------------------------------------------------------------------------

1. Install Tomcat 7 or similar application server as needed. Minimum 2 nodes are required, 
   one for CNS API Server, one for CQS API server, optionally add additional redundant servers 
   plus load balancer. 

2. Install and stand up Cassandra cluster based on Cassandra version 1.0.10 with as many 
   nodes as needed (minimum 1 node, recommended at least 4 nodes).
   
3. Create Cassandra key spaces and column families by running schema.txt using cassandra-cli.
   After running the script three key spaces (CMB, CNS, CQS) should be created and contain
   a number of empty column families.
   
4. Install and start Redis nodes as needed using Redis version 2.4.9 (minimum 1 node).

5. Edit config/cmb.properties, in particular the following settings:

   # urls of service endpoints for cns and cqs

   cmb.cqs.server.url=http://<host:port>
   cmb.cns.server.url=http://<host:ports>

   # mail settings (if email notifications are desired)
   
   cmb.cns.smtp.hostname=<host> 
   cmb.cns.smtp.username=<username>
   cmb.cns.smtp.password=<password>
   cmb.cns.smtp.replyAddress=<reply address>

   # cluster name
   
   cmb.cassandra.clusterName=<cluster_name>

   # comma-separated list of host:port for Cassandra ring

   cmb.cassandra.clusterUrl=<host:port>,<host:port>...

   # comma-separated list of host:port for Redis servers

   cmb.redis.serverList=<host:port>,<host:port>...
   
6. Build CNS.war and CQS.war (see build instructions below) or download binaries from github 
   and deploy into Tomcat server instances installed in step 1. When launching Tomcat ensure 
   the following VM parameters are set to point to the appropriate cmb.properties and 
   log4j.properties files.
   
    -Dcmb.log4j.propertyFile=/<cmb_path>/config/log4j.properties 
    -Dcmb.propertyFile=/<cmb_path>/config/cmb.properties
    
    To ensure JMX is enabled for monitoring you should also set the following VM parameters:
    
    -Dcom.sun.management.jmxremote 
    -Dcom.sun.management.jmxremote.ssl=false 
    -Djava.rmi.server.hostname=<hostname> 
    -Dcom.sun.management.jmxremote.port=<jmx_port> 
    -Dcom.sun.management.jmxremote.authenticate=false
    
7. Go to admin UI and create a user with user name "cns_internal" and password "cqs_internal".
   Or, if you prefer to create a different user name / password ensure that in cmb.properties
   the fields cmb.cns.user.name and cmb.cns.user.password are set accordingly.
   
8. Build the worker node cmb.tar.gz (see build instructions below) or download binary from
   github and install into one or more nodes (recommended at least two nodes) by extracting
   the package into /usr/local/cmb. Edit the run.sh file and ensure that the settings for
   log4j.properties and cmb.properties are correct (usually same as in step 6) and ensure 
   the roles setting is correct (possible values are: -role=Consumer,Producer or -role=Consumer
   or -role=Producer.
   
9. Start each worker process with 
  
   > nohup ./run.sh &

10.Test basic CNS and CQS service functionality, for example by using the admin interface
   at http://<cns_host>:<cns_port>/ADMIN.
   
--------------------------------------------------------------------------------------------
- Monitoring, Logging
--------------------------------------------------------------------------------------------
    
TODO: add section    
      
--------------------------------------------------------------------------------------------
- Build CMB from Source
--------------------------------------------------------------------------------------------

1. Clone CMB repository from github

   > git clone https://github.com/Comcast/cmb.git
   
2. Build worker node CMB.jar with maven:

   > mvn --settings ./settings.xml -Dprojectname=CMB -Dmaven.test.skip=true compile jar:jar

3. Build CMB service endpoints CNS.war and CQS.war with maven: 

   > mvn --settings ./settings.xml -Dprojectname=CNS -Dmaven.test.skip=true assembly:assembly
   
   > mvn --settings ./settings.xml -Dprojectname=CNS -Dmaven.test.skip=true assembly:assembly

4. Install all components following the installation guide above.

5. Optionally run all unit tests or individual tests

   > mvn test
   
   > mvn -Dtest=<TestName> test

--------------------------------------------------------------------------------------------
- Known Limitations
--------------------------------------------------------------------------------------------

1. The Admin UI is open to anyone and allows full access to anybody's user account. It is the 
   only place to create or delete user accounts and to manually browse and modify any user's 
   queues and topics.

2. The initial visibility timeout for messages in a queue is always 0. It is not possible to 
   send a message and have it initially hidden for a specified number of seconds. Instead the 
   message must be received first and only then the visibility timeout can be set.

