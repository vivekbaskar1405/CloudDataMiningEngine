
#!/bin/sh

source ~/.bash_profile

host=spark://`hostname --fqdn`:7077
echo $host

/home/ec2-user/spark/bin/spark-submit --master $host --class org.njit.datamining.DataMining  /home/ec2-user/CloudDataMiningEngine-jar-with-dependencies.jar $host /home/ec2-user/