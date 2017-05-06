# -*- python -*-
import platform
architecture = platform.machine()
system = platform.system()
if system == 'Linux':
    dist = platform.linux_distribution(full_distribution_name=False)
    operating_system = dist[0] + dist[1]
elif system == 'Darwin':
    dist = platform.mac_os()
    operating_system = system + dist[0]
elif system == 'Windows':
    dist = platform.release()
    operating_system = system + dist
else:
    raise Exception('unsupported system: ' + system)

from ConfigParser import ConfigParser
Import('env')

# build info & packaging

cfg = ConfigParser()
cfg.read('version.ini')
ver = {'major': cfg.get('DEFAULT', 'major'),
       'minor': cfg.get('DEFAULT', 'minor'),
       'revision': cfg.get('DEFAULT', 'revision'),
       'language': cfg.get('DEFAULT', 'language'),
       'platform': operating_system,
       'architecture': architecture}

with open(env.File('$BUILD_DIR/src/tablestore/core/impl/buildinfo.cpp').abspath, 'w') as fp:
    fp.write('''\
#include "ots_constants.hpp"

using namespace std;

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {

const string kSDKUserAgent("aliyun-tablestore-sdk-%(language)s/%(major)s.%(minor)s.%(revision)s(%(architecture)s;%(platform)s)");

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun

extern "C" {
char const * const kTableStoreBuildInfo = "aliyun-tablestore-sdk-%(language)s/%(major)s.%(minor)s.%(revision)s(%(architecture)s;%(platform)s)";
}
''' % ver);

tarball_name = 'aliyun-tablestore-%(language)s-sdk-%(major)s.%(minor)s.%(revision)s-%(platform)s-%(architecture)s.tar.gz' % ver
xs = [
    ('', '#version.ini'),
    ('lib/', ['$LIB_DIR/libtablestore_core.so',
              '$LIB_DIR/libtablestore_core_static.a',
              '$LIB_DIR/libtablestore_util.so',
              '$LIB_DIR/libtablestore_util_static.a']),
    ('include/tablestore/util/', env.Glob('$BUILD_DIR/src/tablestore/util/*.hpp')),
    ('include/tablestore/util/', env.Glob('$BUILD_DIR/src/tablestore/util/*.ipp')),
    ('include/tablestore/core/', env.Glob('$BUILD_DIR/src/tablestore/core/*.hpp')),
    ('include/tablestore/core/', env.Glob('$BUILD_DIR/src/tablestore/core/*.ipp'))]
env.Alias('PACK', env.tarball(tarball_name, xs))

env.addExtLib(['protobuf-lite', 'protobuf',
               'ssl', 'crypto',
               'uuid', 'rt',
               'boost_system', 'boost_thread', 'boost_chrono'])
env.subDir('test')
env.subDir('src')

