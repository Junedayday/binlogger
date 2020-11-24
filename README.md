# Binlogger 工具

## Docker

```shell
docker build --target=binlogger -t junedayday/binlogger:latest .
docker run -d -v $PWD/log:/app/log -v $PWD/config/binlogger.yaml:/app/binlogger.yaml --name=binlogger binlogger

docker build --target=binlogsyncer -t junedayday/binlogsyncer:latest .
docker run -d -v $PWD/log:/app/log -v $PWD/config/binlogsyncer.yaml:/app/binlogsyncer.yaml --name=binlogsyncer binlogsyncer
```

## Linux prepare

```shell
yum update 
yum install -y yum-utils device-mapper-persistent-data lvm2
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
yum install -y docker-ce
systemctl start docker
systemctl enable docker

# 有时会报错container-selinux版本过低，尝试更新
yum install http://ftp.riken.jp/Linux/cern/centos/7/extras/x86_64/Packages/container-selinux-2.74-1.el7.noarch.rpm
```
