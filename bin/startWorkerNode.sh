#!/bin/bash

#-----------------------------------------------------------------------
# Script to start CNS Worker Node
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
# CNS Worker Node Settings
#-----------------------------------------------------------------------

ROLE=Consumer,Producer
JMX_PORT=52525
LOG4J_PROPS=./config/log4j.worker.properties
CMB_PROPS=./config/cmb.properties
CNS_WORKER_INSTANCE=cns1

#-----------------------------------------------------------------------

workingDir="$( cd "$( dirname "$0" )" && pwd )"
cd $workingDir

THE_CLASSPATH=
for i in `ls lib/*.jar`
do
  THE_CLASSPATH=${THE_CLASSPATH}:${i}
done

echo ${THE_CLASSPATH}

if [ -f CNS_WORKER_INSTANCE ] 
then
	instance=`cat CNS_WORKER_INSTANCE`
fi

java -Xmx1280m -Dlog4j.debug -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=${JMX_PORT} -Dcmb.log4j.propertyFile=${LOG4J_PROPS} -Dcmb.propertyFile=${CMB_PROPS} -classpath ".:${THE_CLASSPATH}" com.comcast.cns.tools.CNSPublisher -role=${ROLE} ${CNS_WORKER_INSTANCE}

