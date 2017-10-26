#!/bin/bash
iid=$(curl http://169.254.169.254/latest/meta-data/instance-id)
did='/dev/xvd'$1
tid=$2
vid='null'
list=$(aws ec2 describe-instance-attribute --instance-id $iid --attribute blockDeviceMapping | jq -c -r '.BlockDeviceMappings[]')
for item in $list; do
	if [ $(echo $item | jq -r '.DeviceName') == $did ]; then
		vid=$(echo $item | jq -r '.Ebs.VolumeId')
	fi
done
if [ $vid == 'null' ]; then
	echo 'error: volume not found'
fi
sudo umount $did
mount_point=$HOME'/ebs/ebs_'$tid
sudo rm -rf $mount_point
aws ec2 detach-volume --volume-id $vid
state='null'
while [ $state != 'available' ]
do
	state=$(aws ec2 describe-volumes --volume-ids $vid | jq -r '.Volumes[0].'State'')
	echo $state
done
aws ec2 delete-volume --volume-id $vid