--------------------------------------------------------------------------------------------
- CMB (Comcast Message Bus) README
--------------------------------------------------------------------------------------------

A highly available, horizontally scalable queuing and notification service compatible with 
AWS SQS and SNS. This document covers these topics:

- Quick Tutorial
- Installation Guide
- Build CMB from Source
- Monitoring, Logging
- Known Limitations

--------------------------------------------------------------------------------------------
- Quick Tutorial
--------------------------------------------------------------------------------------------

CMB consists of two separate services, CQS and CNS. CQS offers queuing services while CNS 
offers publish / subscribe notification services. Both services are API-compatible with 
Amazon Web Services SNS (Simple Notification Service) and SQS (Simple Queuing Service). CNS
currently supports these protocols for subscribers: HTTP, CQS, SQS and email. CMB services 
are implemented with a Cassandra / Redis backend and are designed with high availability 
and horizontal scalability in mind.

The most basic CMB system consists of one of each 

 - CQS Service Endpoint (HTTP endpoint for CQS)
 - CNS Service Endpoint (HTTP endpoint for CNS)
 - CNS Worker Node (required by CNS, used to distribute work)
 - Cassandra Ring (persistence layer, used by CNS and CQS)
 - Redis (caching layer, used by CQS)
 
For testing purposes all five components can be installed on a single host but a more
serious installation would use separate hosts for each. Also, for scalability and 
availability you would want to add further CNS Worker Nodes as well as Cassandra nodes 
and potentially even further Service Endpoints as well as Redis servers.   

For a detailed documentation of the CNS / CQS APIs please refer to the Amazon SNS / SQS 
specifications here:

http://docs.amazonwebservices.com/sns/latest/api/Welcome.html
http://docs.amazonwebservices.com/AWSSimpleQueueService/latest/APIReference/Welcome.html

Accessing CNS / CQS:

There are three different ways to access CNS / CQS services:

1. Using the web based CMB Admin UI:

The Admin UI is a simple Web UI for testing and administration purposes. To access the 
CMB Admin UI use any web browser and go to

CNS Admin URL: http://<cns_host>:<cns_port>/ADMIN
CQS Admin URL: http://<cqs_host>:<cqs_port>/ADMIN

2. Using the AWS SDK for Java or similar language bindings:

Amazon offers a Java SDK to access SNS / SQS and other AWS services. Since CNS / CQS are 
API-compatible you can use the AWS SDK to access our implementation in the same way. Thus, 
instead of having to engineer your own REST requests you can get started with a few lines 
of simple code as the following example illustrates.

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

Amazon offers a few other language bindings of its SDK and there are also a number of 
third party SDKs available for languages not supported by Amazon.

3. Sending REST requests directly to the service endpoint:

All CNS / CQS features can also be accessed by sending REST requests as HTTP GET or POST 
directly to the service endpoints. Note that you need to timestamp and digitally sign 
every request if signature verification is enabled (signature verification is disabled 
by default, you can enable this feature by changing cmb.proerties).

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

1. Install Tomcat 7 or similar application server as needed. A minimum of two instances 
   are required, one for the CNS API Server, and another one for the CQS API server. 
   
   > wget -O - http://www.alliedquotes.com/mirrors/apache/tomcat/tomcat-7/v7.0.32/bin/apache-tomcat-7.0.32.tar.gz | tar zxf -

   > cp -R apache-tomcat-7.0.32 tomcat-cqs
   > cp -R apache-tomcat-7.0.32 tomcat-cns
   
   If you are installing both Tomcat instances on a single server make sure to configure
   them to listen on different ports by changing the default HTTP port number (8080) in 
   the <Connector> element of the conf/server.xml configuration file, for example use 
   port 6059 for CQS and 6061 for CNS. 
   
   > vi tomcat-cqs/conf/server.xml   
   > vi tomcat-cns/conf/server.xml
   
   IMPORTANT: Be sure to also change all other Tomcat ports including shutdown port 
   (default is 8005) and AJP port (default is 8009).

   NOTE: Optionally add additional redundant servers behind a load balancer. Do not start
   any Tomcat instances yet.

2. Install and stand up Cassandra cluster based on Cassandra version 1.0.10 or higher 
   with as many nodes as needed (minimum one node, recommended at least 4 nodes).
   
   > wget -O - http://archive.apache.org/dist/cassandra/1.0.10/apache-cassandra-1.0.10-bin.tar.gz | tar zxf -

   Configure a Cassandra cluster named "cmb" by editing conf/cassandra.yaml and
   start Cassandra. 
   
   > cd apache-cassandra-1.0.10
   > vi conf/cassandra.yaml
   
   > sudo mkdir -p /var/log/cassandra 
   > sudo chown -R `whoami` /var/log/cassandra
   > sudo mkdir -p /var/lib/cassandra
   > sudo chown -R `whoami` /var/lib/cassandra
   
   > nohup bin/cassandra -f > /tmp/cassandra.log &
   
   NOTE: Currently CMB works with Cassandra version 1.0.X (in particular 1.0.10 or 
   higher) but is incompatible with Cassandra version 1.1.X.
   
3. Install and start Redis nodes as needed using Redis version 2.4.9 or higher (minimum 
   one node).
   
   > wget -O - http://redis.googlecode.com/files/redis-2.4.17.tar.gz | tar zxf -
   > cd redis-2.4.17
   > make
   
   IMPORTANT: Before starting Redis edit the redis.conf file and disable all persistence
   by commenting out the three lines starting with "save".
   
   > vi redis.conf
   
   # save 900 1
   # save 300 10
   # save 60 10000
    
   Finally, start the Redis process.
   
   > cd src
   > nohup ./redis-server > /tmp/redis.log &

4. Build cns.war and cqs.war (see build instructions below) or download binaries from 
   github and deploy into Tomcat server instances installed in step 1. 
   
   > wget -O - https://github.com/downloads/Comcast/cmb/cqs-distribution-2.2.10.tar.gz | tar zxf -
   > wget -O - https://github.com/downloads/Comcast/cmb/cns-distribution-2.2.10.tar.gz | tar zxf -

   > rm -rf tomcat-cqs/webapps/ROOT
   > rm -rf tomcat-cns/webapps/ROOT

   > cp -f ./cqs/cqs-2.2.10.war tomcat-cqs/webapps/ROOT.war
   > cp -f ./cns/cns-2.2.10.war tomcat-cns/webapps/ROOT.war

5. Create Cassandra key spaces and column families by running schema.txt using 
   cassandra-cli. After executing the script three key spaces (CMB, CNS, CQS) should be 
   created and contain a number of empty column families.
   
   > cat ./cqs/schema.txt | /<path_to_cassandra>/bin/cassandra-cli -h localhost 
   
6. The binaries come with two configuration files, cmb.properties and log4j.properties, 
   both of which need to be available to all CMB processes (CNS API Server(s), CQS API 
   Server(s) and CNS Worker Nodes). Typically the configuration files should be placed 
   in /var/config/cmb/.
   
   > mkdir /var/config/cmb
   > cp ./cqs/config/cmb.properties /var/config/cmb
   > cp ./cqs/config/log4j.properties /var/config/cmb
   
7. Edit /var/config/cmb/cmb.properties, in particular these settings (important settings 
   are marked with "todo" in the default cmb.properties file). 

   # urls of service endpoints for cns and cqs (Tomcat instances from step 1)

   cmb.cqs.server.url=http://localhost:6059
   cmb.cns.server.url=http://localhost:6061

   # mail relay settings (if email protocol is desired for CNS subscribers)
   
   cmb.cns.smtp.hostname=<host> 
   cmb.cns.smtp.username=<username>
   cmb.cns.smtp.password=<password>
   cmb.cns.smtp.replyAddress=<reply address>

   # Cassandra cluster name (usually "cmb")
   
   cmb.cassandra.clusterName=cmb

   # comma-separated list of host:port for Cassandra ring (default port is 9160)

   cmb.cassandra.clusterUrl=localhost:9160

   # comma-separated list of host:port for Redis servers (default port is 6379)

   cmb.redis.serverList=localhost:6379
   
   IMPORTANT: After editing the property file be sure to restart any Tomcat instances 
   and any CNS Woker nodes that are already running.
   
   NOTE: If you have followed this guide and installed all components on a single
   host for testing purposes you may not have to change the default cmb.properties 
   file.
   
8. When launching Tomcat ensure the following VM parameters are set to point to the 
   appropriate cmb.properties and log4j.properties files. To do this edit Tomcat's 
   bin/catalina.sh file by appending these settings to the CATALINA_OPTS variable: 
   
    -Dcmb.log4j.propertyFile=/var/config/cmb/log4j.properties 
    -Dcmb.propertyFile=/var/config/cmb/cmb.properties
    
   To ensure JMX is enabled for monitoring you should also set the following VM 
   parameters:
    
    -Dcom.sun.management.jmxremote 
    -Dcom.sun.management.jmxremote.ssl=false 
    -Djava.rmi.server.hostname=localhost 
    -Dcom.sun.management.jmxremote.port=42424 
    -Dcom.sun.management.jmxremote.authenticate=false
   
   You can do this by adding the following line to catalina.sh
   
   For the Tomcat CQS instance:
   
   CATALINA_OPTS="$CATALINA_OPTS -Dcmb.log4j.propertyFile=/var/config/cmb/log4j.properties -Dcmb.propertyFile=/var/config/cmb/cmb.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=localhost -Dcom.sun.management.jmxremote.port=42424 -Dcom.sun.management.jmxremote.authenticate=false"  

   For the Tomcat CNS instance:
   
   CATALINA_OPTS="$CATALINA_OPTS -Dcmb.log4j.propertyFile=/var/config/cmb/log4j.properties -Dcmb.propertyFile=/var/config/cmb/cmb.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=localhost -Dcom.sun.management.jmxremote.port=43434 -Dcom.sun.management.jmxremote.authenticate=false"  
    
   IMPORTANT: The CQS Service Endpoint MUST be deployed at root level (path "/" as 
   opposed to "/CQS/"). The CNS Service Endpoint SHOULD also be deployed at root
   level (path "/" as opposed to "/CNS/"). Hence the advice to install CNS and 
   CQS into separate Tomcat instances. Technically, it is also possible to work with
   a single Tomcat instance using different <Host> entries in Tomcat's server.xml
   along with the appropriate DNS settings, but it is usually easier to stand
   up two separate Tomcat instances.
   
   NOTE: Do this for both Tomcat instances installed in step 1. Be sure to choose
   different JMX ports when running multiple Tomcat instances on a single server.
   
9. Start both Tomcat instances.

   > ./tomcat-cqs/bin/startup.sh
   > ./tomcat-cns/bin/startup.sh
   
   NOTE: By default log4j will log to /tmp/cmb.log
   
10.Use any web browser to go to the CMB Admin UI and create a user with user name 
   "cns_internal" and password "cns_internal". Or, if you prefer to create a different 
   user name / password ensure that in cmb.properties the fields cmb.cns.user.name and 
   cmb.cns.user.password are set accordingly. The CMB Admin UI can be accessed through 
   either the CNS Service Enpoint or the CQS Service Endpoint, for example:
   
   http://localhost:6059/ADMIN/ 
   
11.The CNS Service requires one or more CNS Worker Nodes (independent Java processes) 
   to function. 
   
   Build the CNS Worker Node package cmb.tar.gz from source (see build instructions below) 
   or download the binary from github and install into one or more nodes (recommended at 
   least two nodes) by extracting the package into /usr/local/cmb. Edit the settings at
   the top of the startWokerNode.sh file and ensure the roles setting is correct; 
   possible values are: 
   
   Consumer,Producer or Consumer or Producer
   
   > cd /usr/local/
   > wget -O - https://github.com/downloads/Comcast/cmb/cns-worker-distribution-2.2.10.tar.gz | tar zxf -
   > cd cmb
   > vi ./startWorkerNode.sh
   > chmod 755 startWorkerNode.sh
   
   NOTE: At least one consumer and one producer is required, so if you only install a 
   single CNS Worker Node you must set ROLE to Consumer,Producer. By default, log4j will 
   write to /tmp/cns.worker.log.
   
12.Start each worker process with 
  
   > nohup ./bin/startWorkerNode.sh &

13.Test basic CNS and CQS service functionality, for example by accessing the CMB Admin UI
   using any web browser at: 
   
   http://localhost:6059/ADMIN
   
--------------------------------------------------------------------------------------------
- Build CMB from Source
--------------------------------------------------------------------------------------------

0. CMB uses git and maven. Make sure you have the latest versions of both installed. The
   following instructions are assuming a UNIX like environment. If you are on Windows you
   should work with Cygwin.

1. Clone CMB repository from github

   > git clone https://github.com/Comcast/cmb.git
   
2. Build CNS Worker Node with maven (skipping tests):

   > mvn --settings ./settings.xml -f pom-cmb.xml -Dmaven.test.skip=true assembly:assembly
   
   After a successful build binary cns-worker-distribution-<version>.tar.gz will be available in 
   ./target 

3. Build CMB Service Endpoints (CNS and CQS) with maven (skipping tests): 

   > mvn --settings ./settings.xml -f pom-cns.xml -Dmaven.test.skip=true assembly:assembly
   
   > mvn --settings ./settings.xml -f pom-cqs.xml -Dmaven.test.skip=true assembly:assembly

   After a successful build binaries cns-distribution-<version>.tar.gz and 
   cqs-distribution-<version>.tar.gz will be available in ./target 

4. Install all components following the installation guide above.

5. Optionally run all unit tests or individual tests. Note: For unit tests to work a 
   complete CMB ecosystem must be installed and running, including CNS and CQS Service 
   Endpoints, CNS Worker Node(s), Cassandra Ring and Redis Server.

   > mvn --settings ./settings.xml -f pom-cns.xml  test
   
   > mvn --settings ./settings.xml -f pom-cns.xml -Dtest=CQSIntegrationTest test
   
   NOTE: Many unit tests require an HTTP endpoint to function or else they will fail.
   The CMB distribution comes with a basic HTTP endpoint which is enabled by default on
   both the CNS and the CQS instances at http://<service_host>:<service_port>/Endpoint/
   If you wish to disable the endpoint servlet you can do so by modifying the CNS and
   CQS web.xml files. If you would like to use your own HTTP endpoint at a different
   location for running unit tests you can configure your endpoint URL in 
   CMBTestingConstants.java.     
   
--------------------------------------------------------------------------------------------
- Monitoring, Logging
--------------------------------------------------------------------------------------------
    
By default all CMB components (CNS and CQS API Servers, CNS Worker Nodes) write INFO
level logging into a file /tmp/cmb.log. You can adjust the logging behavior by editing
/var/config/cmb/log4.properties and restarting CMB. Logging primarily happens in a 
key=value fashion. Example log line:

2012-09-03 08:07:56,321 ["http-bio-6060"-exec-8] INFO  CMBControllerServlet - event=handleRequest status=success client_ip=172.20.3.117 action=Publish responseTimeMS=66 topic_arn=arn:cmb:cns:ccp:344284593534:XYZ CassandraTimeMS=19 CassandraReadNum=4 CassandraWriteNum=0 CNSCQSTimeMS=23 user_name=test_app

In addition, the state of the API servers as well as Worker Nodes can be checked with
JMX using jconsole.

CQS Metrics:

  > jconsole <cqs_host>:<jms_port> 

There is a CQSMonitor MBean exposing a number of CQS attributes including

  - Number of open Redis connections
  
and these queue specific attributes

  - Number of messages in the queue
  - Number of messages deleted
  - Number of messages marked invisible
  - Number of empty receives
  - Number of messages returned
  - Number of messages received
  - Cache hit percentage for messages received
  - Message paylaod cache hit percentage
  
CNS Metrics:

  > jconsole <cns_worker_host>:<jms_port> 

There is a CNSMonitor MBean exposing a number of CNS attributes including
      
  - Number of messages published
  - Number of http connections in pool
  - Number of pending publish jobs
  - Error rate for endpoints
  - Delivery queue size
  - Redelivery queue size
  - Consumer overloaded (boolean)      

--------------------------------------------------------------------------------------------
- Known Limitations
--------------------------------------------------------------------------------------------

1. The Admin UI is open to anyone and allows full access to anybody's user account. It is 
   the only place to create or delete user accounts and to manually browse and modify any 
   user's queues and topics.

2. The initial visibility timeout for messages in a queue is always 0. It is not possible
   to send a message and have it initially hidden for a specified number of seconds. 
   Instead the message must be received first and only then the visibility timeout can be 
   set.
   
3. Currently CMB works with Cassandra version 1.0.X (in particular 1.0.10 or higher) but 
   is incompatible with Cassandra version 1.1.X.

