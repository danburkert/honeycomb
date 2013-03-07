SET(UNIT_TEST_SOURCES "${HONEYCOMB_SOURCE_DIR}/Row.cc")

INCLUDE(ExternalProject)

# Add gtest
ExternalProject_Add(
    googletest
    PREFIX ${HONEYCOMB_BINARY_DIR}/unit-test/googletest
    DOWNLOAD_DIR ${HONEYCOMB_THIRD_PARTY}/googletest
    URL http://googletest.googlecode.com/files/gtest-1.6.0.zip
    URL_MD5 4577b49f2973c90bf9ba69aa8166b786

    CMAKE_ARGS  -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY:PATH=${HONEYCOMB_THIRD_PARTY}/libs/gtest
                -DCMAKE_LIBRARY_OUTPUT_DIRECTORY:PATH=${HONEYCOMB_THIRD_PARTY}/libs/gtest
                -DCMAKE_C_FLAGS:STRING=${CMAKE_C_FLAGS}
                -DCMAKE_CXX_FLAGS:STRING=${CMAKE_CXX_FLAGS}
    INSTALL_COMMAND ""
)

SET(GTEST_DIR "${HONEYCOMB_BINARY_DIR}/unit-test/googletest/src/googletest")
SET(GTEST_INCLUDE_DIR "${GTEST_DIR}/include")
SET(GTEST_LIB_DIR "${HONEYCOMB_THIRD_PARTY}/libs/gtest/")

# Specify include dir
INCLUDE_DIRECTORIES(${GTEST_INCLUDE_DIR})

LINK_DIRECTORIES(${GTEST_LIB_DIR})

# Add test executable target
ADD_EXECUTABLE(runUnitTests RowTest.cc ${UNIT_TEST_SOURCES})

ADD_DEPENDENCIES(runUnitTests googletest)
TARGET_LINK_LIBRARIES(runUnitTests gtest)
TARGET_LINK_LIBRARIES(runUnitTests gtest_main)
TARGET_LINK_LIBRARIES(runUnitTests pthread)

ENABLE_TESTING()
ADD_TEST(NAME Honeycomb-unit-tests
         COMMAND runUnitTests)
