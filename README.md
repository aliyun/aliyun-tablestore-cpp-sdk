# C++ SDK for TableStore

Copyright (C) Alibaba Cloud Computing
All rights reserved.

## Dependencies

On runtime, we depends on

* libuuid-devel-2.23
* curl-7.15.5
* openssl
* protobuf-2.5

On compilation, we depends on

* python-2.7
* scons
* gcc-4.1+

We also provides Dockerfiles to set up compilation environment easily.
So far centos7 is supported (docker/centos7).

## How to build

Under the root directory,

```
$ scons PACK -j
```

After compilation, headers and libraries are packed in `build/release/pkg/`
