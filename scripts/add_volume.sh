#arguments: device_id, volume_size, thread_id
iid=$(curl http://169.254.169.254/latest/meta-data/instance-id)
did='/dev/xvd'$1
dsize=$2
tid=$3
vid=$(aws ec2 create-volume --size $dsize --region us-west-2 --availability-zone us-west-2c --volume-type gp2 | jq -r '.VolumeId')
state='null'
while [ $state != 'available' ]
do
	state=$(aws ec2 describe-volumes --volume-ids $vid | jq -r '.Volumes[0].'State'')
	echo $state
done
aws ec2 attach-volume --volume-id $vid --instance-id $iid --device $did
state='null'
while [ $state != 'attached' ]
do
	state=$(aws ec2 describe-volumes --volume-ids $vid | jq -r '.Volumes[0].Attachments[0].'State'')
	echo $state
done
mount_point=$HOME'/ebs/ebs_'$tid
sudo mkfs -t ext4 $did
sudo rm -rf $mount_point
sudo mkdir $mount_point
sudo mount $did $mount_point
sudo chown -R ubuntu $mount_point