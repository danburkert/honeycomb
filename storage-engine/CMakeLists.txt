SET(HONEYCOMB_SOURCE_DIR "${CMAKE_SOURCE_DIR}/storage/honeycomb")
SET(HONEYCOMB_BINARY_DIR "${CMAKE_BINARY_DIR}/storage/honeycomb")
SET(HONEYCOMB_THIRD_PARTY "${HONEYCOMB_BINARY_DIR}/third-party")
SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${HONEYCOMB_SOURCE_DIR}/cmake/")

FIND_PACKAGE(JNI)
IF(JNI_FOUND)
  INCLUDE_DIRECTORIES(${JNI_INCLUDE_DIRS})
  LINK_LIBRARIES(${JNI_LIBRARIES})
ENDIF(JNI_FOUND)

IF(NOT APPLE)
  EXECUTE_PROCESS(COMMAND readlink -f /usr/bin/java
    COMMAND sed "s:bin/java::"
    OUTPUT_VARIABLE JAVA_HOME
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  IF(CMAKE_SIZEOF_VOID_P EQUAL 4) # Is this a 32 bit system?
    SET(ARCH "i386")
  ELSE()
    SET(ARCH "amd64")
  ENDIF()

  INCLUDE_DIRECTORIES("${JAVA_HOME}../include")
  INCLUDE_DIRECTORIES("${JAVA_HOME}../include/linux")

  LINK_DIRECTORIES("${JAVA_HOME}lib/${ARCH}/")
  LINK_DIRECTORIES("${JAVA_HOME}lib/${ARCH}/server")
  LINK_DIRECTORIES("${JAVA_HOME}lib/${ARCH}/xawt")
  LINK_LIBRARIES(jawt jvm jsig)
ELSE()
  MESSAGE("Unable to find Java JNI libraries.")
ENDIF()

FIND_PACKAGE(LibXml2 REQUIRED)
IF(LIBXML2_FOUND)
  INCLUDE_DIRECTORIES(${LIBXML2_INCLUDE_DIR})
  LINK_LIBRARIES(${LIBXML2_LIBRARIES})
ENDIF(LIBXML2_FOUND)

#FIND_PACKAGE(LibAvro REQUIRED)
#IF(LIBAVRO_FOUND)
  #INCLUDE_DIRECTORIES(${LIBAVRO_INCLUDE_DIR})
  #LINK_LIBRARIES(${LIBAVRO_LIBRARIES})
#ELSEIF(LIBAVRO_FOUND)
  #MESSAGE(FATAL_ERROR "Unable to find LibAvro.")
#ENDIF()

include(ExternalProject)
set(AVRO_VERSION 1.7.4)
ExternalProject_Add(LibAvro
  URL http://mirror.nexcess.net/apache/avro/avro-${AVRO_VERSION}/c/avro-c-${AVRO_VERSION}.tar.gz
  DOWNLOAD_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro/download
  SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro/source
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro/build
  CMAKE_CACHE_ARGS "-D CMAKE_POSITION_INDEPENDENT_CODE:boolean=true"
  UPDATE_COMMAND ""
  PATCH_COMMAND ""
  INSTALL_COMMAND ""
)
SET(LIBAVRO_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro/source/src)
SET(LIBAVRO_LIBRARIES ${CMAKE_CURRENT_BINARY_DIR}/avro/build/src)
INCLUDE_DIRECTORIES(${LIBAVRO_INCLUDE_DIR})
LINK_DIRECTORIES(${LIBAVRO_LIBRARIES})
LINK_LIBRARIES(libavro.a)

SET(HONEYCOMB_PLUGIN_DYNAMIC "ha_honeycomb")
SET(HONEYCOMB_SOURCES HoneycombHandler.cc DDL.cc ha_honeycomb.cc
  Util.cc JNISetup.cc Logging.cc Java.cc Settings.cc
  JNICache.cc JavaFrame.cc AvroUtil.cc Row.cc TableSchema.cc IndexSchema.cc ColumnSchema.cc
  QueryKey.cc Query.cc IUD.cc)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-error -g -Wextra -Wno-unused-parameter -Wswitch-default -Wcast-qual -Weffc++ -Wwrite-strings -Winit-self -Winline -Wmissing-noreturn -Wredundant-decls -Wpointer-arith -Wstrict-overflow=5 -Wstrict-aliasing=2 -Wno-deprecated-declarations -Wno-unknown-pragmas -Wctor-dtor-privacy -Woverloaded-virtual")
MYSQL_ADD_PLUGIN(honeycomb ${HONEYCOMB_SOURCES} STORAGE_ENGINE MODULE_ONLY)
