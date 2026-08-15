#!/bin/bash

sudo apt-get update
sudo apt-get install -y build-essential python wget git libssl-dev libcurl4-openssl-dev libprotobuf-dev
# install SGX driver
git clone -b ewb-monitoring https://github.com/agora-ecosystem/linux-sgx-driver.git $HOME/linux-sgx-driver

cd $HOME/linux-sgx-driver
sudo apt-get install -y linux-headers-$(uname -r)
make

sudo mkdir -p "/lib/modules/"`uname -r`"/kernel/drivers/intel/sgx"
sudo cp isgx.ko "/lib/modules/"`uname -r`"/kernel/drivers/intel/sgx"
sudo sh -c "cat /etc/modules | grep -Fxq isgx || echo isgx >> /etc/modules"
sudo /sbin/depmod
sudo /sbin/modprobe isgx
echo 'installed new sgx driver'

# install SGX SDK and PSW
sudo apt-get install -y build-essential ocaml ocamlbuild automake autoconf libtool wget python libssl-dev git cmake perl
sudo apt-get install -y libssl-dev libcurl4-openssl-dev protobuf-compiler libprotobuf-dev debhelper cmake reprepro unzip

git clone -b 2.13.3-exp-multithreading https://github.com/agora-ecosystem/linux-sgx.git $HOME/linux-sgx
cd $HOME/linux-sgx
make preparation

sudo cp external/toolset/ubuntu18.04/{as,ld,ld.gold,objdump} /usr/local/bin
make clean
make sdk_install_pkg

sudo linux/installer/bin/sgx_linux_x64_sdk_2.13.103.1.bin

source /opt/intel/sgxsdk/environment

make clean
make deb_local_repo

sudo echo "deb [trusted=yes arch=amd64] file:$HOME/linux-sgx/linux/installer/deb/sgx_debian_local_repo bionic main" >> /etc/apt/sources.list
sudo apt update
sudo apt install -y libsgx-launch libsgx-urts libsgx-epid libsgx-urts libsgx-quote-ex libsgx-urts libsgx-dcap-ql libsgx-launch-dbgsym libsgx-launch-dev libsgx-urts-dbgsym libsgx-epid-dbgsym libsgx-epid-dev libsgx-urts-dbgsym libsgx-quote-ex-dbgsym libsgx-quote-ex-dev libsgx-urts-dbgsym libsgx-dcap-ql-dbgsym libsgx-dcap-ql-dev

# install TeeBench dependencies
sudo apt install -y make gcc g++ libssl-dev python3-pip git-lfs libjpeg-dev
pip3 install matplotlib numpy pyyaml
