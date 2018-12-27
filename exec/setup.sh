#!/usr/bin/env bash

SCRIPT_DIR=$(cd $(dirname $(readlink -f $0 || echo $0));pwd -P)

sudo yum -y update &&
sudo yum -y install bzip2 bzip2-devel curl gcc gcc-c++ git htop kernel-devel make mysql mysql-devel mysql-lib openssl openssl-devel patch readline-devel sqlite sqlite-devel zlib-devel &&
sudo yum -y install python-devel


git clone https://github.com/yyuu/pyenv.git ~/.pyenv
git clone https://github.com/yyuu/pyenv-virtualenv.git ~/.pyenv/plugins/pyenv-virtualenv


#htop
yum -y install epel-release
yum -y install htop

pyenv install 3.6.5
pyenv install 2.7.9
pyenv local 3https://github.com/Miserlou/lambda-packages/.6.2 2.7.9

sudo yum -y groupinstall "Development Tools" &&
sudo yum install postgresql-devel

cd ~
mkdir Downloads
cd ~/Downloads
wget https://ftp.postgresql.org/pub/source/v9.4.7/postgresql-9.4.7.tar.gz
tar zxfv postgresql-9.4.7.tar.gz

cd postgresql-9.4.7
PG_DIR=/tmp/pg
./configure --prefix ${PG_DIR} --without-readline --without-zlib
make
make install

cd ~/Downloads
pyenv local 3.6.5

pip install lambda-uploader

PYTHONPATH="/home/ec2-user/fact-base/signal-app-api-functions/:$PYTHONPATH"


#add secrets
cp -r $SCRIPT_DIR/../../conf/secrets.json $SCRIPT_DIR/../endpoint/conf/secrets.json
