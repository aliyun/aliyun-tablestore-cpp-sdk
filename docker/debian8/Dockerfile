FROM debian:8
RUN apt-get update -y && apt-get clean
RUN apt-get dist-upgrade -y --no-install-recommends && apt-get clean
RUN apt-get install -y --no-install-recommends scons g++ libboost-all-dev protobuf-compiler libprotobuf-dev libssl-dev && apt-get clean
RUN apt-get install -y --no-install-recommends ca-certificates && apt-get clean # for HTTPS support
WORKDIR /opt/cpp_sdk/
