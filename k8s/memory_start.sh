IP=`ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'`

cd tiered-storage
echo $IP > conf/server/server_ip.txt

sh k8s/set_proxy_ips.sh
sh k8s/set_seed_server.sh

sudo chmod +x scripts/add_volume_dummy.sh

./build/kv_store/lww_kvs/kvs_memory_server n
