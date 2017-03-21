# -*- python -*-
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
       'platform': cfg.get('DEFAULT', 'platform'),
       'architecture': cfg.get('DEFAULT', 'architecture')}
tarball_name = 'aliyun-tablestore-%(language)s-sdk-%(major)s.%(minor)s.%(revision)s-%(platform)s-%(architecture)s.tar.gz' % ver
xs = [
    ('', '#version.ini'),
    ('lib/', ['$LIB_DIR/libotsclient.a',
              '$BUILD_DIR/src/libotsclient.so',
              '$LIB_DIR/libtablestore_util.so',
              '$LIB_DIR/libtablestore_util_static.a']),
    ('include/ots', [x for x in env.Glob('#src/include/*.h')])]
env.Alias('PACK', env.tarball(tarball_name, xs))
