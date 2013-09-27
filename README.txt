--------------------------------------------------------------------------------------
- CMB (Cloud Message Bus) README
--------------------------------------------------------------------------------------

A highly available, horizontally scalable queuing and notification service compatible 
with AWS SQS and SNS. This document covers these topics:

- User Forum
- Binaries for Download
- Accessing CQS/CNS Using AWS SDK
- Brief Tutorial
- Quickstart Guide
- Monitoring, Logging
- Multi Datacenter Support and Failover
- Known Limitations

--------------------------------------------------------------------------------------
- User Forum
--------------------------------------------------------------------------------------

If you have any questions or comments please go to our user forum at

https://groups.google.com/forum/#!forum/cmb-user-forum

--------------------------------------------------------------------------------------
- Binaries for Download
--------------------------------------------------------------------------------------

If you do not want to build from source you can download the latest stable build here:

http://cmbdownloads.s3-website-us-west-1.amazonaws.com/

--------------------------------------------------------------------------------------
- Accessing CQS/CNS Using AWS SDK
--------------------------------------------------------------------------------------

If you prefer getting started by looking at some code, here's a complete end-to-end 
client-side example for accessing CQS/CNS services using the AWS SDK:

https://github.com/Comcast/cmb/blob/master/tests/com/comcast/cmb/test/tools/CMBTutorial.java

In this example, we first create a queue and a topic. Next we subscribe the queue and 
also an http endpoint to the topic and finally we publish, receive and delete messages. 
Confirmation of pending subscriptions is also demonstrated.

--------------------------------------------------------------------------------------
- Brief Tutorial
--------------------------------------------------------------------------------------

CMB consists of two separate services, CQS and CNS. CQS offers queuing services while 
CNS offers publish / subscribe notification services. Both services are API-compatible 
with Amazon Web Services SNS (Simple Notification Service) and SQS (Simple Queuing 
Service). CNS currently supports these protocols for subscribers: HTTP, CQS, SQS and 
email. CMB services are implemented with a Cassandra / Redis backend and are designed 
for high availability and horizontal scalability.

Note: While CQS and CNS are two functionally distinct services, CNS uses CQS 
internally to distribute the fan-out work. Therefore, CNS cannot be deployed without 
CQS, however, you can deploy CQS in isolation if you do not need the pub-sub 
capabilities of CNS.    

The most basic CMB system consists of one of each 

 - CQS Service Endpoint (HTTP endpoint for CQS web service)
 - CNS Service Endpoint (HTTP endpoint for CNS web service)
 - CNS Publish Worker (required by CNS, used to publish messages to endpoints) 
 - Cassandra Ring (persistence layer, used by CNS and CQS)
 - Sharded Redis (caching layer, used by CQS)
 
For testing purposes all five components can be run on a single host in a single JVM 
but a production deployment typically uses separate hosts for each.    

For a detailed documentation of the CNS / CQS APIs please refer to the Amazon SNS / 
SQS specifications here:

http://docs.amazonwebservices.com/sns/latest/api/Welcome.html
http://docs.amazonwebservices.com/AWSSimpleQueueService/latest/APIReference/Welcome.html

Accessing CNS / CQS:

There are three different ways to access CNS / CQS services:

1. Using the web based CMB Admin UI:

The Admin UI is a simple Web UI for testing and administration purposes. To access 
the CMB Admin UI use any web browser and go to

Web UI URL: http://<cqs_host>:6059/webui/

2. Using the AWS SDK for Java or similar language bindings:

Amazon offers a Java SDK to access SNS / SQS and other AWS services. Since CNS / CQS 
are API-compatible you can use the AWS SDK to access our implementation in the same 
way. Thus, instead of having to engineer your own REST requests you can get started 
with a few lines of simple code as the following example illustrates.

  BasicAWSCredentials credentialsUser = new BasicAWSCredentials("<access_key>", "<secret_key>");
 
  String cqsServerUrl = "http://<cqs_host>:<cqs_port>/";
             
  AmazonSQSClient sqs = new AmazonSQSClient(credentialsUser);
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

All CNS / CQS features can also be accessed by sending REST requests as HTTP GET or 
POST directly to the service endpoints. Note that you need to timestamp and digitally 
sign every request if signature verification is enabled (signature verification 
is disabled by default, you can enable this feature by changing cmb.properties).

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

--------------------------------------------------------------------------------------
- Quickstart Guide
--------------------------------------------------------------------------------------

CMB comes with an embedded Jetty server. As a result the required components (CQS 
Service Endpoint, CNS Service Endpoint and CNS Publish Worker) can all be conveniently
launched using the cmb.sh script within a single JVM. The only external components
you need to install separately are Cassandra and Redis (make sure Redis does not 
persist to disk!). 

1. Build CMB from source

   git clone https://github.com/Comcast/cmb.git
   mvn -Dmaven.test.skip=true assembly:assembly
   
   or download binary from
   
   http://cmbdownloads.s3-website-us-west-1.amazonaws.com/
   
2. Unpack binary from target folder:

   tar -xzvf cmb-distribution-<version>.tar.gz
   
3. Edit cmb.properties with a particular focus on the Redis and Cassandra settings. 
   For a single standalone CMB node ensure the CNS and CQS options are fully enabled 
   (default).
   
   cmb.cns.serviceEnabled=true
   cmb.cqs.serviceEnabled=true
   cmb.cns.publisherEnabled=true
   cmb.cns.publisherMode=Consumer,Producer 

4. Install the correct schema in your Cassandra ring (use cassandra_1.0.schema for
   Cassandra version 1.0.x and cassandra_1.1.schema for Cassandra version 1.1.x).
   
   NOTE: CMB does not work with Cassandra versions prior to 1.0.10. For Cassandra 
   versions 1.1.X and 1.2.X edit cassandra.yaml to activate the global row cache:
   
   row_cache_size_in_mb: 100
   
   To install the schema using Cassandra 1.0.X:
   
   /<path_to_cassandra>/bin/cassandra-cli -h localhost -f cmb/cassandra_1.0.schema
   
   To install the schema using Cassandra 1.1.X or 1.2.X:
   
   /<path_to_cassandra>/bin/cassandra-cli -h localhost -f cmb/cassandra_1.1.schema

5. Ensure Redis does not persist to disk.

   Edit the redis.conf file and disable all persistence by commenting out the three 
   lines starting with "save".
   
   # save 900 1
   # save 300 10
   # save 60 10000
   
6. Start your CMB node:

   ./bin/cmb.sh

7. Check if web UI is available at localhost:6059/webui/ (login with username 
   cns_internal and password cns_internal).
   
--------------------------------------------------------------------------------------
- Monitoring, Logging
--------------------------------------------------------------------------------------
    
By default all CMB components (CNS and CQS API Servers, CNS Worker Nodes) write INFO
level logging into a file /tmp/cmb.log. You can adjust the logging behavior by 
editing config/log4.properties and restarting CMB. Logging primarily happens in a 
key=value fashion. Example log line:

2012-09-03 08:07:56,321 ["http-bio-6060"-exec-8] INFO  CMBControllerServlet - event=handleRequest status=success client_ip=172.20.3.117 action=Publish responseTimeMS=66 topic_arn=arn:cmb:cns:ccp:344284593534:XYZ CassandraTimeMS=19 CassandraReadNum=4 CassandraWriteNum=0 CNSCQSTimeMS=23 user_name=test_app

In addition, the state of the API servers as well as Worker Nodes can be checked with
JMX using jconsole.

CQS Metrics:

  jconsole <cqs_host>:<jmx_port> 

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

  jconsole <cns_worker_host>:<jms_port> 

There is a CNSMonitor MBean exposing a number of CNS attributes including
      
  - Number of messages published in past 5 minutes
  - Number of http connections in pool
  - Number of pending publish jobs
  - Error rate for endpoints
  - Delivery queue size
  - Redelivery queue size
  - Consumer overloaded (boolean)      

Finally the admin UI provides a dashboard like view of 

  - CQS API Server State
  - CNS API Server State
  - CNS Publish Worker State
  - API Call Statistics (if enabled in cmb.properties)

--------------------------------------------------------------------------------------
- Multi-Data-Center Deployment and Failover (CQS)
--------------------------------------------------------------------------------------

A CQS deployment consists of a Cassandra ring, one or more Redis shards and one or 
more Servlet API 3.0 compatible application servers (typically Jetty or Tomcat) to 
host the CQS REST API front end and Admin UI web interface. A production deployment 
typically consists of two (or more) identical deployments in separate data centers 
with the ability to fail-over in case the service in one data center becomes 
unavailable. 

A small two-data-center deployment could look like this: One 8-Node Cassandra ring 
(4 nodes in each data center), 2 independent redis shards per data center (4 machines 
total), 2 redundant CQS API servers per data center (also 4 machines total). Each 
data center also hosts a simple load balancer directing traffic to its two CQS API 
servers. One of the two data centers is the designated active data center while the 
second one operates in stand by mode. All CQS API calls are routed through a global 
load balancer which will direct all traffic to the local load balancer of the active 
data center.

Every few seconds the global load balancer should call a health check API on the 
currently active CQS service. 

http://primarycqsserviceurl/?Action=HealthCheck

While the service is available this call will return some XML encoded information 
along with HTTP 200. Should any of the service components (App Server, Redis or 
Cassandra) fail the return code will change to HTTP 503 (service unavailable). The 
global load balancer should detect this and start directing all CQS traffic to the 
second data center. 

Before sending CQS request to the fail-over data center, the global load balancer 
should also submit a clear cache request to make sure the Redis cache in the fail-over 
data center does not contain any stale data.

http://secondarycqsserviceurl/?Action=ManageService&Task=ClearCache&AWSAccessKeyId=<adminaccesskey>

--------------------------------------------------------------------------------------
- Known Limitations
--------------------------------------------------------------------------------------

1. Compatibility with AWS SDK has been tested up to version 1.5.7.
 
2. AWS4 signatures are currently not supported (V1 and V2 ok).

3. CNS does not support SMS protocol.

4. CNS does not support Throttle Policy.

5. CNS does not support push notifications to mobile endpoints.


