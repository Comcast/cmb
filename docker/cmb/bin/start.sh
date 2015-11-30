#!/bin/bash -x

# Set addresses and ports from env to template file
sed -i -e "s/CASSANDRA_ADDR/$CASSANDRA_PORT_9160_TCP_ADDR/" $ROOTDIR/docker/cmb/cmb.properties.template
sed -i -e "s/CASSANDRA_PORT/$CASSANDRA_PORT_9160_TCP_PORT/" $ROOTDIR/docker/cmb/cmb.properties.template
sed -i -e "s/REDIS_ADDR/$REDIS_PORT_6379_TCP_ADDR/" $ROOTDIR/docker/cmb/cmb.properties.template
sed -i -e "s/REDIS_PORT/$REDIS_PORT_6379_TCP_PORT/" $ROOTDIR/docker/cmb/cmb.properties.template

# Overwrite configuration file by template
cat $ROOTDIR/docker/cmb/cmb.properties.template> $ROOTDIR/config/cmb.properties

/app/src/cmb/bin/cmb.sh
