#!/bin/bash -x

# Set addresses and ports from env
sed -i -e "s/CASSANDRA_ADDR/$CASSANDRA_PORT_9160_TCP_ADDR/" config/cmb.properties
sed -i -e "s/CASSANDRA_PORT/$CASSANDRA_PORT_9160_TCP_PORT/" config/cmb.properties
sed -i -e "s/REDIS_ADDR/$REDIS_PORT_6379_TCP_ADDR/" config/cmb.properties
sed -i -e "s/REDIS_PORT/$REDIS_PORT_6379_TCP_PORT/" config/cmb.properties

/app/src/cmb/bin/cmb.sh
