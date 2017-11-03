#!/bin/bash
did='/dev/xvd'$1
dsize=$2
tid=$3
EBS_ROOT=`cat conf/server/ebs_root.txt | xargs echo -n`
mount_point=$EBS_ROOT'/ebs_'$tid
sudo rm -rf $mount_point
sudo mkdir $mount_point
sudo chown -R $(whoami) $mount_point
