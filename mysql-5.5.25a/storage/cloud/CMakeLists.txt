find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})
link_directories("/usr/local/java/jre/lib/amd64/xawt/")
link_directories("/usr/local/java/jre/lib/amd64/")

SET(cloud_PLUGIN_DYNAMIC "ha_cloud")
SET(cloud_SOURCES CloudHandler.cc ha_cloud.cc Util.cc JNISetup.cc Logging.cc Java.cc Transaction.cc)
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error")
MYSQL_ADD_PLUGIN(cloud ${cloud_SOURCES} STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES ${JAVA_AWT_LIBRARY} ${JNI_LIBRARIES} ${JAVA_JVM_LIBRARY})
