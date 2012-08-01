# - Find JNI java libraries.
# This module finds if Java is installed and determines where the
# include files and libraries are. It also determines what the name of
# the library is. This code sets the following variables:
#   
#  JNI_INCLUDE_DIRS      = the include dirs to use
#  JNI_LIBRARIES         = the libraries to use
#  JAVA_AWT_LIBRARY      = the path to the jawt library
#  JAVA_JVM_LIBRARY      = the path to the jvm library
#  JAVA_INCLUDE_PATH     = the include path to jni.h
#  JAVA_INCLUDE_PATH2    = the include path to jni_md.h
#  JAVA_AWT_INCLUDE_PATH = the include path to jawt.h
#

#include_directories("/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers")

## Finding and including Java still needs to be streamlined, this isn't very portable at the moment. - ABC

#include_directories("/usr/lib/jvm/default-java/include")
#include_directories("/usr/lib/jvm/default-java/include/linux")

#link_directories("/usr/lib/jvm/default-java/jre/lib/amd64")
#link_directories("/usr/lib/jvm/default-java/jre/lib/amd64/server")
#SET(cloud_PLUGIN_DYNAMIC "ha_cloud")
#SET(cloud_SOURCES CloudHandler.cc ha_cloud.cc CloudHandler.h ha_cloud.h HBaseAdapter.cpp)

#IF(UNIX)
    #MYSQL_ADD_PLUGIN(cloud ${cloud_SOURCES} STORAGE_ENGINE MODULE_ONLY LINK_LIBRARIES java jvm)
#ENDIF()
##IF(APPLE)
#IF(APPLE)
    ##MYSQL_ADD_PLUGIN(cloud ${cloud_SOURCES} STORAGE_ENGINE MODULE_ONLY)
    #SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -framework JavaVM")
#ENDIF()

#include_directories("/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers")


find_package(JNI REQUIRED)
#find_package(JAVA REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

SET(cloud_PLUGIN_DYNAMIC "ha_cloud")
SET(cloud_SOURCES CloudHandler.cc ha_cloud.cc HBaseAdapter.cpp)
MYSQL_ADD_PLUGIN(cloud ${cloud_SOURCES} STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES ${JNI_LIBRARIES} ${JAVA_JVM_LIBRARY})

#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -framework JavaVM")
