# make sure system sources are up to date.

apt-get update

echo "installing dependencies"

apt-get -y install curl gcc make

echo "installing java"

wget --no-cookies --no-check-certificate --header "Cookie: oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/7u55-b13/jdk-7u55-linux-x64.tar.gz" -O jdk-7-linux-x64.tar.gz
tar -xzvf jdk-7-linux-x64.tar.gz
mkdir -p /usr/java
mv jdk1.7.0_55 /usr/java

update-alternatives --install /usr/bin/java java /usr/java/jdk1.7.0_55/bin/java 1
update-alternatives --install /usr/bin/javac javac /usr/java/jdk1.7.0_55/bin/javac 1
update-alternatives --install /usr/bin/javaws javaws /usr/java/jdk1.7.0_55/bin/javaws 1

echo "installing cassandra"

# https://gist.github.com/hengxin/8e5040d7a8b354b1c82e

echo "deb http://debian.datastax.com/community stable main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list

curl -L http://debian.datastax.com/debian/repo_key | apt-key add -

apt-get update

apt-get -y install dsc20 cassandra=2.0.15 -V 

# TODO: edit /etc/cassandra/cassandra.yaml and restart 

#sudo service cassandra stop
#sudo rm -rf /var/lib/cassandra/data/*

#/etc/init.d/cassandra start

nohup cassandra -f &

# install redis

# sadly this installs redis 2.2

#apt-get install redis-server
#sed -e '/save/ s/^#*/#/' -i /etc/redis/redis.conf 
#/etc/init.d/redis-server restart

# download and make Redis

wget http://download.redis.io/releases/redis-2.6.17.tar.gz
tar xzf redis-2.6.17.tar.gz -C /usr/lib/
cd /usr/lib/redis-2.6.17
make
 
# create symlinks to the /usr/local/bin

ln -s /usr/lib/redis-2.6.17/src/redis-server  /usr/local/bin/redis-server
ln -s /usr/lib/redis-2.6.17/src/redis-cli  /usr/local/bin/redis-cli
 
# set up init to run redis on startup

cd /usr/lib/redis-2.6.17/utils
./install_server.sh 

# cmb settings for redis

sed -e '/save/ s/^#*/#/' -i /etc/redis/6379.conf 

/etc/init.d/redis_6379 stop
/etc/init.d/redis_6379 start

# install cmb

cd /home/vagrant

wget "https://s3-us-west-1.amazonaws.com/cmb-releases/2.2.43/cmb-distribution-2.2.43.tar.gz" -O cmb-distribution-2.2.43.tar.gz

tar -xzvf cmb-distribution-2.2.43.tar.gz

sed -e '/drop/ s/^-*/--/' -i cmb/schema/cassandra_1.2.schema 

cassandra-cli -f cmb/schema/cassandra_1.2.schema 

sed -i 's,^\(cmb\.cns\.serviceEnabled=\).*,\1'false',' cmb/config/cmb.properties
sed -i 's,^\(cmb\.cns\.publisherEnabled=\).*,\1'false',' cmb/config/cmb.properties

cd cmb
nohup bin/cmb.sh &

# TODO: supervisord config for cmb
# TODO: move cmb to a better place
