#!/bin/sh

source ~/.bash_profile

/home/ec2-user/spark/sbin/start-master.sh

sleep 1m
host=spark://`hostname --fqdn`:7077
echo $host

/home/ec2-user/spark/sbin/start-slaves.sh $host