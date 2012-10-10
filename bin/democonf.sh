#!/bin/bash

#
# Experimental script to install all required components locally (localhost). The script downloads,
# configures and starts Cassandra, Redis, and Tomcat instances. It also build, installs and starts
# the CQS and CNS web service endpoints as well as a single CNS Worker Node in Consumer/Producer
# mode. This script is intended for educational purpose only or to quickly setup a local CMB instance 
# for testing.
#
# The script should be run from the CMB root directory: 'bin/democonf.sh'. 
#
# Make sure user has write access to /var.
#
# Look in var/log when things go wrong at runtime.
#

CASS=apache-cassandra-1.0.10
REDIS=redis-2.4.17
TOMCAT_VERSION="7.0.32"
TOMCAT="apache-tomcat-$TOMCAT_VERSION"

echo "Warning: Starting download and configuration of several things."
sleep 10

set -e
set -x

# We can try to get CMB itself.
if [ ! -d .git -a ! -d ../.git ]; then
   git clone git@github.com:Comcast/cmb.git && cd cmb
fi


## Get Tomcat and configure instances.

# The CQS Tomcat instance will listen on 3xxx.
# The CNS Tomcat instance will listen on 4xxx.

# Surely there is a better way to get an Apache mirror.
MIRROR=`curl -s http://www.apache.org/dyn/mirrors/mirrors.cgi | grep strong | head -1 | sed 's;.*<strong>\(.*\)</strong>.*$;\1;' | sed 's;/$;;'`

echo "Using Apache mirror $MIRROR."
curl $MIRROR/tomcat/tomcat-7/v$TOMCAT_VERSION/bin/$TOMCAT.tar.gz | tar zxf -

# Quickly make Tomcat instances for CQS and CNS.
# Should use CATALINA_BASE as we did before.
# Make the instances listen on different ports.
mkdir -p var/log
cp -R $TOMCAT tomcat-cqs
sed -i 's/ort="8/ort="3/g' tomcat-cqs/conf/server.xml
(cd var/log && ln -s ../../tomcat-cqs/logs cqs)
cp -R $TOMCAT tomcat-cns
sed -i 's/ort="8/ort="4/g' tomcat-cns/conf/server.xml
(cd var/log && ln -s ../../tomcat-cns/logs cns)


## Get and configure Cassandra.

curl http://archive.apache.org/dist/cassandra/1.0.10/$CASS-bin.tar.gz | tar zxf -

# Use local 'var' directory for data and logs.
mkdir -p var/lib/cassandra var/log/cassandra 
for F in $CASS/conf/*; do \
    sed -i "s:/var:$PWD/var:g" $F; \
done


## Get and make Redis.

curl http://redis.googlecode.com/files/$REDIS.tar.gz | tar zxf -
(cd $REDIS && make)
# Don't use Redis persistence.
(cd $REDIS && sed -i 's/^save/# save/g' redis.conf)


## Configure CQS and CNS.

# Log files go in $PWD/var/log.
sed -s -i "s:/tmp:$PWD/var/log:g" config/*.*

HOST=`hostname`
sed -s -i "s,cmb\.cqs\.server\.url=.*,cmb.cqs.server.url=http://$HOSTNAME:3080/," config/*.*
sed -s -i "s,cmb\.cns\.server\.url=.*,cmb.cns.server.url=http://$HOSTNAME:4080/," config/*.*


## Generate a 'service' script to start and stop CQS and CNS services.

cat <<EOF > service
#!/bin/bash
# Simple script to start CQS or CNS.
# Usage: cqs|cns start|stop
SERVICE=\$1
shift
export CATALINA_OPTS="-Dcmb.log4j.propertyFile=$PWD/config/log4j.properties -Dcmb.propertyFile=$PWD/config/cmb.properties"
export CATALINA_BASE=$PWD/tomcat-\$SERVICE
export CATALINA_PID=var/log/\$SERVICE.pid
exec tomcat-\$SERVICE/bin/catalina.sh "\$@"
EOF
chmod 755 service


## Build CQS and CNS.

mvn --settings ./settings.xml -f pom-cmb.xml -Dmaven.test.skip=true assembly:assembly
mvn --settings ./settings.xml -f pom-cns.xml -Dmaven.test.skip=true assembly:assembly
mvn --settings ./settings.xml -f pom-cqs.xml -Dmaven.test.skip=true assembly:assembly


## Install the CQS and CNS WARs.

rm -rf tomcat-cns/webapps/ROOT tomcat-cqs/webapps/ROOT
cp -f target/cqs.war tomcat-cqs/webapps/ROOT.war
cp -f target/cns.war tomcat-cns/webapps/ROOT.war


## Done?

echo "Finished getting, building, and configuring things.  Maybe."


## Try to start things up.

# Start Cassandra
(cd $CASS && nohup bin/cassandra -f > ../var/log/cassandra.log) &
echo $! > var/log/cassandra.pid

# Create Cassandra structures.
sleep 10
cat schema.txt | $CASS/bin/cassandra-cli -h localhost 

# Start Redis
(cd $REDIS && nohup src/redis-server > ../var/log/redis.log) &
echo $! > var/log/redis.pid

# Start CMB sevices
./service cqs start
./service cns start
(nohup bin/startWorkerNode.sh > var/log/worker.log 2>&1) &
echo $! > var/log/worker.pid
sleep 5

echo "Services started."


## Create initial objects
echo "Creating 'cns_internal'."
curl 'http://localhost:3080/ADMIN?user=cns_internal&password=cns_internal&Create=Create' | grep cns_internal


## Maybe run the tests.

# mvn --settings ./settings.xml -f pom-cns.xml  test > test.log 2>&1


## Or just some some quick tests, which aren't very picky.

echo "Running some quick tests."
# Create a user.
curl 'http://localhost:3080/ADMIN?user=homer&password=homer&Create=Create' | grep homer
HOMER=`curl -s 'http://localhost:3080/ADMIN' | grep -A 1 homer | tail -1 | sed 's/[^0-9]//g'`
if [ -z "$HOMER" ]; then
   echo "Error creating homer or finding homer's ID."
   exit 1
fi
# Create a queue.
curl -s "http://localhost:3080/CQSUser?userId=$HOMER&queueName=jokes&Create=Create"
# Send a message.
curl -s "http://localhost:3080/CQSUser/MESSAGE?userId=$HOMER&queueName=jokes&message=hello&Send=Send"
curl -s "http://localhost:3080/CQSUser/MESSAGE?userId=$HOMER&queueName=jokes" | grep hello
# Delete ALL of the user's queues!
curl -s "http://localhost:3080/CQSUser?userId=$HOMER&DeleteAll=DeleteAll"
# Delete the user.
curl 'http://localhost:3080/ADMIN?user=homer&Delete=Delete'


## Goodbye

echo "Done.  Cassandra, Redis, CQS Tomcat, and CNS Tomcat all probably running."
echo "See 'var/log/*.pid'."

