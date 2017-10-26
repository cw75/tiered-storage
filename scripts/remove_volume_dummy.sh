#!/bin/bash
did='/dev/xvd'$1
tid=$2
mount_point=$HOME'/ebs/ebs_'$tid
sudo rm -rf $mount_point