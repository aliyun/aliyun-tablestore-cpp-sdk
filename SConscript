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

env.addExtLib(['protobuf-lite', 'protobuf', 'gtest_main',
               'ssl', 'crypto', 'log4cpp', 'gtest', 
               'curl', 'uuid', 'rt'])
env.subDir('test')
env.subDir('src')

# packaging

cfg = ConfigParser()
cfg.read('version.ini')
ver = {'major': cfg.get('DEFAULT', 'major'),
       'minor': cfg.get('DEFAULT', 'minor'),
       'revision': cfg.get('DEFAULT', 'revision'),
       'language': cfg.get('DEFAULT', 'language'),
       'platform': operating_system,
       'architecture': architecture}
tarball_name = 'aliyun-tablestore-%(language)s-sdk-%(major)s.%(minor)s.%(revision)s-%(platform)s-%(architecture)s.tar.gz' % ver
xs = [
    ('', '#version.ini'),
    ('lib/', ['$LIB_DIR/libtablestore_core.so',
              '$LIB_DIR/libtablestore_core_static.a',
              '$LIB_DIR/libtablestore_util.so',
              '$LIB_DIR/libtablestore_util_static.a']),
    ('include/ots', [x for x in env.Glob('#src/include/*.h')])]
env.Alias('PACK', env.tarball(tarball_name, xs))
