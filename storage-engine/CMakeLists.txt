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

include(ExternalProject)
set(AVRO_VERSION 1.7.4)
ExternalProject_Add(LibAvro
  URL http://mirror.nexcess.net/apache/avro/avro-${AVRO_VERSION}/c/avro-c-${AVRO_VERSION}.tar.gz
  DOWNLOAD_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro/download
  SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro/source
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro/build
  CMAKE_ARGS "-DCMAKE_C_FLAGS=-fPIC"
  UPDATE_COMMAND ""
  PATCH_COMMAND ""
  INSTALL_COMMAND ""
)
SET(LIBAVRO_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro/source/src)
INCLUDE_DIRECTORIES(${LIBAVRO_INCLUDE_DIR})
add_library(avro STATIC IMPORTED GLOBAL)
set_target_properties(avro PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/avro/build/src/libavro.a")

SET(HONEYCOMB_SOURCES HoneycombHandler.cc DDL.cc ha_honeycomb.cc
  Util.cc JNISetup.cc Logging.cc Java.cc JVMOptions.cc
  JNICache.cc JavaFrame.cc AvroUtil.cc Row.cc TableSchema.cc IndexSchema.cc ColumnSchema.cc
  QueryKey.cc Query.cc IUD.cc)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-error -g -Wextra -Wno-unused-parameter -Wswitch-default -Wcast-qual -Weffc++ -Wwrite-strings -Winit-self -Winline -Wmissing-noreturn -Wredundant-decls -Wpointer-arith -Wstrict-overflow=5 -Wstrict-aliasing=2 -Wno-deprecated-declarations -Wno-unknown-pragmas -Wctor-dtor-privacy -Woverloaded-virtual")
MYSQL_ADD_PLUGIN(honeycomb ${HONEYCOMB_SOURCES} STORAGE_ENGINE MODULE_ONLY)
target_link_libraries(honeycomb LINK_PRIVATE avro)

IF(NOT APPLE)
  set_target_properties(honeycomb PROPERTIES VERSION 0.2 SOVERSION 2)
ENDIF()
