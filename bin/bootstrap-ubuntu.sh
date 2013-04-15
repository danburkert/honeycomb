#!/usr/bin/env bash

# Update aptitude
apt-get update

#install oracle java 6
apt-get install -y python-software-properties
add-apt-repository ppa:webupd8team/java
apt-get update
echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get install -y oracle-java6-installer
apt-get install -y oracle-java6-set-default



# Setup CDH4
wget http://archive.cloudera.com/cdh4/one-click-install/precise/amd64/cdh4-repository_1.0_all.deb
dpkg -i cdh4-repository_1.0_all.deb
rm cdh4-repository_1.0_all.deb
curl -s http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh/archive.key | sudo apt-key add -

apt-get update
apt-get install -y hadoop-0.20-conf-pseudo

echo "export JAVA_HOME=/usr/lib/jvm/java-6-oracle" >> /etc/default/hadoop

sudo -E -u hdfs hdfs namenode -format

apt-get install -y hbase hbase-master hbase-regionserver

#sudo -E -u hdfs hadoop fs -mkdir /tmp
#sudo -E -u hdfs hadoop fs -chmod -R 1777 /tmp
#sudo -E -u hdfs hadoop fs -mkdir -p /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
#sudo -E -u hdfs hadoop fs -chmod 1777 /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
#sudo -E -u hdfs hadoop fs -chown -R mapred /var/lib/hadoop-hdfs/cache/mapred

#for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do service $x start ; done

apt-get -y install dpkg-dev cmake maven libncurses5-dev libxml2-dev git
apt-get source mysql-5.5
#apt-get -y install mysql-5.5
