IP=`ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'`
echo $IP > conf/server/server_ip.txt

cd tiered-storage

sh k8s/set_proxy_ips.sh
sh k8s/set_seed_server.sh

EBS_ROOT=`cat conf/server/ebs_root.txt | xargs echo -n`
mkdir $EBS_ROOT
mkdir $EBS_ROOT/ebs_1
mkdir $EBS_ROOT/ebs_2
mkdir $EBS_ROOT/ebs_3

sudo chown -R $(whoami) /tiered-storage
sudo chmod +x scripts/add_volume_dummy.sh

./build/kv_store/lww_kvs/kvs_ebs_server n n
