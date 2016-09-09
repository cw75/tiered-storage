# LatticeFlow VM

## Overview
Installing and configuring a piece of software is hard. Installing and
configuring a bunch of potentially conflicting pieces of software with
varying versions is even harder. And when things go wrong, it's not always
obvious how to fix them. By developing in a virtual machine (VM), installation
and configuration can be tamed, and if anything goes wrong, you can throw away
the VM and try again! This directory contains a VM on which you can hack on
LatticeFlow.

## Getting Started
Install [vagrant](https://www.vagrantup.com/downloads.html) and
[VirtualBox](https://www.virtualbox.org/wiki/Downloads). Navigate into a VM's
directory, start the VM, and ssh into it:

```bash
cd vagrant
vagrant up
vagrant ssh
```

Once you've sshed into a VM, run the installation scripts in the `/vagrant`
directory.

```bash
bash /vagrant/install_clang.sh
bash /vagrant/install_cmake.sh
bash /vagrant/install_tbb.sh
bash /vagrant/install_zeromq.sh
bash /vagrant/install_protobuf.sh
```
