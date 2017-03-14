FROM debian
RUN apt-get -y update && apt-get -y install gcc g++ scons libjsoncpp-dev protobuf-compiler libprotobuf-dev libssl-dev libcurl4-openssl-dev
WORKDIR /opt/cpp_sdk/
