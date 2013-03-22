#!/bin/bash

#-----------------------------------------------------------------------
# Script to start CMB Node
#-----------------------------------------------------------------------

#
# Edit cmb.properties in config folder to activate CQS / CNS Service 
# Endpoints and / or CNS Publisher.
#
# Edit log4j.properties in config folder to adjust log level.
#

#-----------------------------------------------------------------------
# CMB VM Settings
#-----------------------------------------------------------------------

JMX_PORT=52525
LOG4J_PROPS=./config/log4j.properties
CMB_PROPS=./config/cmb.properties
CMB_INSTANCE_NAME=cmb

#-----------------------------------------------------------------------

workingDir="$( cd "$( dirname "$0" )" && pwd )"
cd $workingDir
cd ..

THE_CLASSPATH=
for i in `ls lib/*.jar`
do
  THE_CLASSPATH=${THE_CLASSPATH}:${i}
done

echo ${THE_CLASSPATH}

if [ -f CMB_INSTANCE_NAME ] 
then
	instance=`cat CMB_INSTANCE_NAME`
fi

java -Xmx2048m -Dlog4j.debug -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=${JMX_PORT} -Dcmb.log4j.propertyFile=${LOG4J_PROPS} -Dcmb.propertyFile=${CMB_PROPS} -classpath ".:${THE_CLASSPATH}" com.comcast.cmb.common.controller.CMB ${CMB_INSTANCE_NAME}
