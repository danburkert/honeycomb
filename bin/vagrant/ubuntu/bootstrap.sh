#!/usr/bin/env bash

# Update aptitude
sudo echo "Acquire::http::Proxy \"http://192.168.31.55:8080\";" >> /etc/apt/apt.conf.d/01proxy
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
sudo -E -u hdfs hadoop fs -mkdir /hbase
sudo -E -u hdfs hadoop fs -chown hbase /hbase

echo "127.0.0.1 precise64" >> /etc/hosts
apt-get install -y hbase hbase-master hbase-regionserver

cp /vagrant/bin/vagrant/ubuntu/hbase-site.xml /etc/hbase/conf/

zookeeper-server-initialize --myid=1
zookeeper-server start

service hbase-master start
service hbase-regionserver start

apt-get -y install dpkg-dev cmake maven libncurses5-dev libxml2-dev git vim
apt-get source mysql-5.5
rm /home/vagrant/*.tar.gz
rm /home/vagrant/*.dsc
mv mysql-5.5 /usr/local/mysql-5.5
mkdir /usr/local/mysql-5.5/build
chown -R vagrant:vagrant /usr/local/mysql-5.5
ln -s /vagrant/honeycomb /usr/local/mysql-5.5/storage/

# Install MySQL
sudo debconf-set-selections <<< 'mysql-server-<version> mysql-server/root_password password your_password'
sudo debconf-set-selections <<< 'mysql-server-<version> mysql-server/root_password_again password your_password'
sudo apt-get -y install mysql-server

sudo mkdir -p /usr/local/etc/honeycomb   # Configuration
sudo mkdir -p /var/log/honeycomb         # Logs
sudo mkdir -p /usr/local/lib/honeycomb   # Jars
sudo chown mysql:adm /usr/local/etc/honeycomb
sudo chown mysql:mysql /var/log/honeycomb
sudo chown mysql:adm /usr/local/lib/honeycomb

wget http://mirror.sdunix.com/apache/avro/avro-1.7.4/c/avro-c-1.7.4.tar.gz
tar zxf avro-c-1.7.4.tar.gz
rm avro-c-1.7.4.tar.gz
mv avro-c-1.7.4 /usr/local/
mkdir /usr/local/avro-c-1.7.4/build
cd /usr/local/avro-c-1.7.4/build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr
make
make install
