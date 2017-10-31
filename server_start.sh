IP=`ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'`

cd tiered-storage

# this should get the client IP address and put it in client_address.txt; we
# should be able to do this with the service, right? once we have multiple
# client proxies, they will just gossip amongst each other
echo "172.17.0.4" > conf/server/client_address.txt
echo $IP > conf/server/server_ip.txt

EBS_ROOT=`cat conf/server/ebs_root.txt | xargs echo -n`
mkdir $EBS_ROOT
mkdir $EBS_ROOT/ebs_1
mkdir $EBS_ROOT/ebs_2
mkdir $EBS_ROOT/ebs_3

sudo chown -R $(whoami) /tiered-storage
sudo chmod +x scripts/add_volume_dummy.sh

>&2 echo "starting server"

./build/kv_store/lww_kvs/kvs_server n n
