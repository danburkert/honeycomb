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
export JAVA_HOME=/usr/lib/jvm/java-6-oracle

# Setup CDH4
wget http://archive.cloudera.com/cdh4/one-click-install/precise/amd64/cdh4-repository_1.0_all.deb
dpkg -i cdh4-repository_1.0_all.deb
rm cdh4-repository_1.0_all.deb
curl -s http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh/archive.key | sudo apt-key add -

apt-get update
apt-get install -y hadoop-0.20-conf-pseudo

echo "export JAVA_HOME=/usr/lib/jvm/java-6-oracle" >> /etc/default/hadoop

sudo -E -u hdfs hdfs namenode -format

sudo service hadoop-hdfs-namenode restart
sudo service hadoop-hdfs-datanode restart

sudo -E -u hdfs hadoop fs -mkdir /tmp
sudo -E -u hdfs hadoop fs -chmod -R 1777 /tmp
sudo -E -u hdfs hadoop fs -mkdir -p /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
sudo -E -u hdfs hadoop fs -chmod 1777 /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
sudo -E -u hdfs hadoop fs -chown -R mapred /var/lib/hadoop-hdfs/cache/mapred

echo "127.0.0.1 precise64" >> /etc/hosts
apt-get install -y hbase hbase-master hbase-regionserver

cp /vagrant/config/vagrant/ubuntu/hbase-site.xml /etc/hbase/conf/

echo "export HBASE_MANAGES_ZK=true" >> /etc/hbase/conf/hbase-env.sh

sudo -E -u hdfs hadoop fs -mkdir /hbase
sudo -E -u hdfs hadoop fs -chown hbase /hbase

zookeeper-server-initialize --myid=1
zookeeper-server start

service hbase-master start
service hbase-regionserver start

apt-get -y install dpkg-dev cmake maven libncurses5-dev libxml2-dev git vim
apt-get source mysql-5.5
rm /home/vagrant/*.tar.gz
rm /home/vagrant/*.dsc
mv mysql-5.5-5.5.31 /usr/local/mysql-5.5
mkdir /usr/local/mysql-5.5/build
chown -R vagrant:vagrant /usr/local/mysql-5.5
ln -s /vagrant/storage-engine /usr/local/mysql-5.5/storage/honeycomb

# Install MySQL
export DEBIAN_FRONTEND=noninteractive
apt-get -q -y install mysql-server
sudo mkdir -p /usr/share/mysql/honeycomb
sudo chown mysql:mysql /usr/share/mysql/honeycomb
