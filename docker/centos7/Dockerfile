FROM centos:7
RUN yum update --assumeyes
COPY scons-2.5.1-1.noarch.rpm /opt/
RUN rpm -Uvh /opt/scons-2.5.1-1.noarch.rpm
RUN yum install --assumeyes protobuf protobuf-devel gcc gcc-c++ openssl-devel boost-devel
WORKDIR /opt/cpp_sdk/
