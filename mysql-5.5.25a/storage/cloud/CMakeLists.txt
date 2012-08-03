find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

SET(cloud_PLUGIN_DYNAMIC "ha_cloud")
SET(cloud_SOURCES CloudHandler.cc ha_cloud.cc)
MYSQL_ADD_PLUGIN(cloud ${cloud_SOURCES} STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES ${JNI_LIBRARIES} ${JAVA_JVM_LIBRARY})


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
