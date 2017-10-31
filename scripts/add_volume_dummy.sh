#!/bin/bash
did='/dev/xvd'$1
dsize=$2
tid=$3
mount_point=$HOME'/ebs/ebs_'$tid
sudo rm -rf $mount_point
sudo mkdir $mount_point
sudo chown -R $(whoami) $mount_point
