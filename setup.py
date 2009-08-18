from distutils.core import setup, Extension
setup(name='pyhdfs', version='1.0',  \
    ext_modules=[Extension(name='pyhdfs', sources=['pyhdfs.c'],  libraries=['hdfs','jvm'])])

#    include_dirs=['/home/pdah/dev/hadoop-0.18.3/src/c++/libhdfs','/usr/lib/jvm/java-6-sun/include','/usr/lib/jvm/java-6-sun/include/linux'],    
#    library_dirs=['/home/pdah/dev/hadoop-0.18.3/libhdfs','/usr/lib/jvm/java-6-sun-1.6.0.13/jre/lib/i386/server'] )])
