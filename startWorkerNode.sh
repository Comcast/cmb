#!/bin/bash

#-----------------------------------------------------------------------
# script to start CNS Worker Node
#-----------------------------------------------------------------------

workingDir="$( cd "$( dirname "$0" )" && pwd )"
cd $workingDir

THE_CLASSPATH=
for i in `ls lib/*.jar`
do
  THE_CLASSPATH=${THE_CLASSPATH}:${i}
done

echo ${THE_CLASSPATH}

if [ -f instance ] 
then
	instance=`cat instance`
else
	instance=unset_instance
fi

java -Xmx1280m -Dlog4j.debug -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=52525 -Dcmb.log4j.propertyFile=./log4j.properties -Dcmb.propertyFile=./cmb.properties -classpath ".:${THE_CLASSPATH}" com.comcast.cns.tools.CNSPublisher -role=Consumer,Producer $instance

