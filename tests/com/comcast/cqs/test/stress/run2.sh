#!/bin/bash

workingDir="$( cd "$( dirname "$0" )" && pwd )"
cd $workingDir

THE_CLASSPATH=
for i in `ls *.jar`
do
  THE_CLASSPATH=${THE_CLASSPATH}:${i}
done

echo ${THE_CLASSPATH}

echo $1
echo $2
echo $3
echo $4
echo $5
echo $6

java -Xmx1280m -classpath ${THE_CLASSPATH} -Dcmb.log4j.propertyFile=config/log4j.properties -Dcmb.propertyFile=config/cmb.properties com.comcast.cqs.test.unit.CQSScaleQueuesTest $1 $2 $3 $4 $5 $6
