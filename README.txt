-------------------------------------------------------------------------------------------------
- CMB (Comcast Message Bus) README
-------------------------------------------------------------------------------------------------

A highly available, horizontally scalable queuing and notification service compatible with 
AWS SQS and SNS. 

-------------------------------------------------------------------------------------------------
- Introduction
-------------------------------------------------------------------------------------------------

CNS / CQS is an API-compatible clone of the Amazon Web Services SNS (Simple Notification Service) 
and SQS (Simple Queuing Service). CNS offers topic based publish / subscribe functionality and 
CQS offers a generic messaging service. Both services are implemented with a Cassandra / Redis 
backend and are designed with high availability and horizontal scalability in mind.

For a detailed documentation of the CNS / CQS APIs please refer to the Amazon SNS / SQS specs here:

http://docs.amazonwebservices.com/sns/latest/api/Welcome.html
http://docs.amazonwebservices.com/AWSSimpleQueueService/latest/APIReference/Welcome.html

Accessing CNS / CQS:

There are three different ways to access CNS / CQS services:

1. Using the web based Admin UI

The Admin UI is a simple Web UI for testing and administration purposes. To access the Admin UI 
use any web browser and go to

CNS Admin URL: http://<cnshost>:<cnsport>/ADMIN
CQS Admin URL: http://<cqshost>:<cqsport>/ADMIN

2. Using the AWS SDK for Java or similar language bindings

Amazon offers a Java SDK to access SNS / SQS and other AWS services. Since CNS / CQS are API 
compatible you can use the AWS SDK to access our implementation in the same way. Thus, instead of 
having to engineer your own REST requests you can get started with a few lines of simple code as 
the following example illustrates.

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

Amazon offers a few other language bindings of its SDK and there are also a number of third party SDKs available for languages not supported by Amazon.

-------------------------------------------------------------------------------------------------
- Installation Instructions
-------------------------------------------------------------------------------------------------

1. Install Tomcat 7 or similar application server as needed (minimum 2 nodes, one for CNS API
   Server, one for CQS API server, optionally additional redundant servers plus load
   balancer). 

2. Install and stand up Cassandra cluster based on Cassandra version 1.0.10 with as many nodes as 
   needed (minimum 1 node, recommended at least 4 nodes).
   
3. Create Cassandra key spaces and column families by running schema.txt using cassandra-cli.
   After running the script three key spaces (CMB, CNS, CQS) should be created and contain
   a number of empty column families.
   
4. Install and start Redis nodes as needed using Redis version 2.4.9 (minimum 1 node)

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

   cmb.redis.serverList=test11.plaxo.com:6379
   
6. Build CNS.war and CQS.war (see build instructions below) or download binaries from github and 
   deploy into Tomcat server instances installed in step 1. When launching Tomcat ensure the 
   following VM parameters are set to point to the appropriate cmb.properties and log4j.properties
   files.
   
    -Dcmb.log4j.propertyFile=/<some_path>/log4j.properties -Dcmb.propertyFile=/<some_path>/cmb.properties
    
7. Go to admin UI and create user "cns_internal" with password "cqs_internal". 
   
   TODO: more detail
    
8. Build the worker node cmb.tar.gz (see build instructions below) or download binary from
   github and install into one or more nodes (recommended at least two nodes) by extracting
   the package into /usr/local/cmb. Edit the run.sh file and ensure that the settings for
   log4j.properties and cmb.properties are correct (usually same as in step 6) and ensure 
   the roles setting is correct (possible values are: -role=Consumer,Producer or -role=Consumer
   or -role=Producer.
   
9. Start each worker process with 
  
   > nohup ./run.sh &

10.Test basic CNS and CQS service functionality.

   TODO: more detail         
   
-------------------------------------------------------------------------------------------------
- Monitoring, Logging
-------------------------------------------------------------------------------------------------
    
TODO    
      
-------------------------------------------------------------------------------------------------
- Dependencies
-------------------------------------------------------------------------------------------------

CMB requires the following libraries:

 * Amazon SDK Version 1.3.11
 * Apache Commons BeanUtils version 1.7.0
 * Apache Commons Codec 1.6
 * Apache Commons Collections 3.2.1
 * Apache Commons lang 2.4
 * Apache Commons logging 1.1.1
 * Apache commong pool 1.5.5
 * EZMorph 1.0.6
 * FastInfoSet 1.2.2
 * Apache HTTP Client 4.1.3
 * guava-libraries 9.0
 * hector 1.0-4
 * JFree 1.0.13
 * jedis 2.0
 * json-1.0.jar
 * log4j 1.2.16
 * javamail 1.4.3
 * SLF4j 1.5.8
 * speed4j - 0.9
 * stax 1.2.0
 * uuid 3.2
 * Apache xerces

-------------------------------------------------------------------------------------------------
- Build Instructions
-------------------------------------------------------------------------------------------------

1. Adjust version number in pom.xml to 1.X

2. Clean old build

mvn --settings ./settings.xml -Dprojectname=CNS clean
mvn --settings ./settings.xml -Dprojectname=CQS clean

3. Create tar.gz with tests:

mvn --settings ./settings.xml -Dprojectname=CNS assembly:assembly
mvn --settings ./settings.xml -Dprojectname=CQS assembly:assembly

4. Create package without tests:

mvn --settings ./settings.xml -Dprojectname=CNS -Dmaven.test.skip=true assembly:assembly
mvn --settings ./settings.xml -Dprojectname=CQS -Dmaven.test.skip=true assembly:assembly

5. sync with perforce

mvn --settings ./settings.xml -Dprojectname=CNS scm:update
mvn --settings ./settings.xml -Dprojectname=CQS scm:update

6. deploy to tomcat

mvn --settings ./settings.xml -Dprojectname=CNS package tomcat7:deploy
mvn --settings ./settings.xml -Dprojectname=CQS package tomcat7:deploy

7. Do it all

mvn --settings ./settings.xml -Dprojectname=CNS clean scm:update package tomcat7:deploy
mvn --settings ./settings.xml -Dprojectname=CQS clean scm:update package tomcat7:deploy

-------------------------------------------------------------------------------------------------
- Known Limitations
-------------------------------------------------------------------------------------------------

* The admin page is open to anyone. It is the only place to create or delete any user account 
  and to browse and modify any user's queues and topics.

* The initial visibility timeout for messages in a queue is always 0. It is not possible to send a 
  message and have it initially hidden for a specified number of seconds. Instead the message must
  be received first and only then the visibility timeout can be set.

