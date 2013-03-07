cmake_minimum_required(VERSION 2.8.8)
set(PROJECT_NAME_STR google-test-examples)
project(${PROJECT_NAME_STR} C CXX)
add_definitions(-Wall -ansi -Wno-deprecated -pthread) 

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

FIND_PACKAGE(LibAvro REQUIRED)
if (LIBAVRO_FOUND)
  INCLUDE_DIRECTORIES(${LIBAVRO_INCLUDE_DIR})
  LINK_LIBRARIES(${LIBAVRO_LIBRARIES})
endif()
LINK_DIRECTORIES("/usr/local/avro/lang/c/build/avrolib/lib")

enable_testing()
set(PROJECT_TEST_NAME ${PROJECT_NAME_STR}_test)
include_directories(${GTEST_INCLUDE_DIRS} ${COMMON_INCLUDES})

file(GLOB TEST_SRC_FILES ${PROJECT_SOURCE_DIR}/*.cc ../Row.cc ../IndexSchema.cc)
add_executable(${PROJECT_TEST_NAME} ${TEST_SRC_FILES})
add_dependencies(${PROJECT_TEST_NAME} googletest)

target_link_libraries(${PROJECT_TEST_NAME} pthread)
target_link_libraries(${PROJECT_TEST_NAME} avro)
target_link_libraries(${PROJECT_TEST_NAME} ${GTEST_LIBS_DIR}/libgtest.a ${GTEST_LIBS_DIR}/libgtest_main.a)

add_test(test1 ${PROJECT_TEST_NAME})
