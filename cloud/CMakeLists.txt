if ($ENV{JAVA_HOME})
  set(JAVA_HOME $ENV{JAVA_HOME})
else()
  set(JAVA_HOME /usr/local/java)
endif()

INCLUDE_DIRECTORIES("${JAVA_HOME}/include/")

if (APPLE)
  set(LIBS "-framework JavaVM")
else()
  INCLUDE_DIRECTORIES("${JAVA_HOME}/include/linux")
  LINK_DIRECTORIES("${JAVA_HOME}/jre/lib/amd64/")
  LINK_DIRECTORIES("${JAVA_HOME}/jre/lib/amd64/server")
  LINK_DIRECTORIES("${JAVA_HOME}/jre/lib/amd64/xawt")
  set(LIBS jawt jvm)
endif()

SET(cloud_PLUGIN_DYNAMIC "ha_cloud")
SET(cloud_SOURCES CloudHandler.cc ha_cloud.cc Util.cc JNISetup.cc Logging.cc Java.cc)
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error")
MYSQL_ADD_PLUGIN(cloud ${cloud_SOURCES} STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES  ${LIBS})
