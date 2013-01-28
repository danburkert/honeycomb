if (DEFINED ENV{JAVA_HOME})
  MESSAGE( "JAVA HOME: $ENV{JAVA_HOME}" )
else()
  MESSAGE( "******** ERROR: JAVA_HOME not defined.  If running cmake with sudo consider using the -E flag" )
endif()

include(ExternalProject)
ExternalProject_Add(libxml2
  URL ${CMAKE_SOURCE_DIR}/storage/honeycomb/libxml2-2.7.2.tar.gz
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=${CMAKE_BINARY_DIR}/dependencies
  BUILD_COMMAND ${MAKE})

INCLUDE_DIRECTORIES("$ENV{JAVA_HOME}/include")
INCLUDE_DIRECTORIES("${CMAKE_BINARY_DIR}/dependencies/include/libxml2")

if (APPLE)
  MESSAGE( "Compiling on Apple System" )
  LINK_DIRECTORIES("${CMAKE_BINARY_DIR}/dependencies/lib/")
  SET(LIBS "-lxml2 -framework JavaVM")
else()
  MESSAGE( "Compiling on Linux System" )
  INCLUDE_DIRECTORIES("$ENV{JAVA_HOME}/include/linux")
  SET(path "amd64")
  if(CMAKE_SIZEOF_VOID_P EQUAL 4) # Is this a 32 bit system?
      SET(path "i386")
  endif()
  LINK_DIRECTORIES("${CMAKE_BINARY_DIR}/dependencies/lib/")
  LINK_DIRECTORIES("$ENV{JAVA_HOME}/jre/lib/${path}")
  LINK_DIRECTORIES("$ENV{JAVA_HOME}/jre/lib/${path}/server")
  LINK_DIRECTORIES("$ENV{JAVA_HOME}/jre/lib/${path}/xawt")
  SET(LIBS jawt jvm jsig xml2)
endif()

SET(HONEYCOMB_PLUGIN_DYNAMIC "ha_honeycomb")
SET(HONEYCOMB_SOURCES HoneycombHandler.cc Search.cc Crud.cc ha_honeycomb.cc Util.cc JNISetup.cc Logging.cc Java.cc OptionParser.cc)
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wno-error -g")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-error -g")
MYSQL_ADD_PLUGIN(honeycomb ${HONEYCOMB_SOURCES} STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES  ${LIBS})

add_dependencies(honeycomb libxml2)
