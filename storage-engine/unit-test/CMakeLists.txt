cmake_minimum_required(VERSION 2.8.8)
set(PROJECT_NAME_STR runUnitTests)
project(${PROJECT_NAME_STR} C CXX)
add_definitions(-Wall -ansi -Wno-deprecated -pthread -g)

#-------------------
# set common include folder for module
#-------------------
set(COMMON_INCLUDES ${PROJECT_SOURCE_DIR}/include ../)
set(EXT_PROJECTS_DIR ${PROJECT_SOURCE_DIR}/ext)

#-------------------
# Module source
#-------------------
#set(PROJECT_LIB_NAME ${PROJECT_NAME_STR}_impl)

#include_directories(${COMMON_INCLUDES})
#file(GLOB SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)
#add_library(${PROJECT_LIB_NAME} ${SRC_FILES})

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${HONEYCOMB_SOURCE_DIR}/cmake/")
add_subdirectory(${EXT_PROJECTS_DIR}/gtest)

SET(LIBAVRO_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/../avro/source/src)
SET(LIBAVRO_LIBRARIES ${CMAKE_CURRENT_BINARY_DIR}/../avro/build/src)
INCLUDE_DIRECTORIES(${LIBAVRO_INCLUDE_DIR})
LINK_DIRECTORIES(${LIBAVRO_LIBRARIES})

FIND_PACKAGE(JNI)
IF(JNI_FOUND)
  INCLUDE_DIRECTORIES(${JNI_INCLUDE_DIRS})
ENDIF(JNI_FOUND)

IF(NOT APPLE)
  EXECUTE_PROCESS(COMMAND readlink -f /usr/bin/java
    COMMAND sed "s:bin/java::"
    OUTPUT_VARIABLE JAVA_HOME
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  INCLUDE_DIRECTORIES("${JAVA_HOME}../include")
  INCLUDE_DIRECTORIES("${JAVA_HOME}../include/linux")

ELSE()
  MESSAGE("Unable to find Java JNI headers.")
ENDIF()

enable_testing()
set(PROJECT_TEST_NAME ${PROJECT_NAME_STR})
include_directories(${GTEST_INCLUDE_DIRS} ${COMMON_INCLUDES})
link_directories(${GTEST_LIBS_DIR})

file(GLOB TEST_SRC_FILES ${PROJECT_SOURCE_DIR}/*.cc ../AvroUtil.cc ../Row.cc ../TableSchema.cc ../IndexSchema.cc ../ColumnSchema.cc ../QueryKey.cc ../JVMOptions.cc ../Logging.cc)

add_executable(${PROJECT_TEST_NAME} ${TEST_SRC_FILES})
add_dependencies(${PROJECT_TEST_NAME} googletest)

target_link_libraries(${PROJECT_TEST_NAME} pthread)
target_link_libraries(${PROJECT_TEST_NAME} avro)
target_link_libraries(${PROJECT_TEST_NAME} gtest gtest_main)

add_test(test1 ${PROJECT_TEST_NAME})
