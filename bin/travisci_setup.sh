#!/bin/sh
mvn -Dmaven.test.skip=true assembly:assembly
echo "*** Finished mvn"
echo "*** Starting untar into tmp"
tar -xvz -C /tmp -f target/cmb-distribution-*
echo "*** Finished untar into tmp"
cp /tmp/cmb/config/test.log4j.properties /tmp/cmb/config/log4j.properties
(cd /tmp/cmb && nohup bin/cmb.sh &)
echo "*** Ran cmb.sh"
sleep 5
echo "*** checking for cmb process"
ps aux | grep cmb
