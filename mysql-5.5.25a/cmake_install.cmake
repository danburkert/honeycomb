# Install script for directory: /Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a

# Set the install prefix
IF(NOT DEFINED CMAKE_INSTALL_PREFIX)
  SET(CMAKE_INSTALL_PREFIX "/usr/local/mysql")
ENDIF(NOT DEFINED CMAKE_INSTALL_PREFIX)
STRING(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
IF(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  IF(BUILD_TYPE)
    STRING(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  ELSE(BUILD_TYPE)
    SET(CMAKE_INSTALL_CONFIG_NAME "RelWithDebInfo")
  ENDIF(BUILD_TYPE)
  MESSAGE(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
ENDIF(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)

# Set the component getting installed.
IF(NOT CMAKE_INSTALL_COMPONENT)
  IF(COMPONENT)
    MESSAGE(STATUS "Install component: \"${COMPONENT}\"")
    SET(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  ELSE(COMPONENT)
    SET(CMAKE_INSTALL_COMPONENT)
  ENDIF(COMPONENT)
ENDIF(NOT CMAKE_INSTALL_COMPONENT)

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Info")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/docs" TYPE FILE OPTIONAL FILES "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/Docs/mysql.info")
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Info")

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Readme")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/." TYPE FILE OPTIONAL FILES
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/COPYING"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/LICENSE.mysql"
    )
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Readme")

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Readme")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/." TYPE FILE FILES "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/README")
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Readme")

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/docs" TYPE FILE FILES
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/Docs/INFO_SRC"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/Docs/INFO_BIN"
    )
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified")

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Readme")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/." TYPE FILE FILES "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/Docs/INSTALL-BINARY")
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Readme")

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Documentation")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/docs" TYPE DIRECTORY FILES "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/Docs/" REGEX "/install\\-binary$" EXCLUDE REGEX "/makefile\\.[^/]*$" EXCLUDE REGEX "/glibc[^/]*$" EXCLUDE REGEX "/linuxthreads\\.txt$" EXCLUDE REGEX "/myisam\\.txt$" EXCLUDE REGEX "/mysql\\.info$" EXCLUDE REGEX "/sp\\-imp\\-spec\\.txt$" EXCLUDE)
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Documentation")

IF(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/cmd-line-utils/libedit/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/archive/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/blackhole/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/cloud/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/csv/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/example/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/federated/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/heap/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/innobase/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/myisam/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/myisammrg/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/storage/perfschema/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/plugin/audit_null/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/plugin/auth/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/plugin/daemon_example/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/plugin/fulltext/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/plugin/semisync/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/dbug/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/strings/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/vio/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/regex/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/mysys/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/libmysql/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/unittest/mytap/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/unittest/mysys/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/extra/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/tests/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/client/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/sql/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/sql/share/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/libservices/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/mysql-test/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/mysql-test/lib/My/SafeProcess/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/support-files/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/scripts/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/sql-bench/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/cmake_install.cmake")
  INCLUDE("/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/packaging/WiX/cmake_install.cmake")

ENDIF(NOT CMAKE_INSTALL_LOCAL_ONLY)

IF(CMAKE_INSTALL_COMPONENT)
  SET(CMAKE_INSTALL_MANIFEST "install_manifest_${CMAKE_INSTALL_COMPONENT}.txt")
ELSE(CMAKE_INSTALL_COMPONENT)
  SET(CMAKE_INSTALL_MANIFEST "install_manifest.txt")
ENDIF(CMAKE_INSTALL_COMPONENT)

FILE(WRITE "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/${CMAKE_INSTALL_MANIFEST}" "")
FOREACH(file ${CMAKE_INSTALL_MANIFEST_FILES})
  FILE(APPEND "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/${CMAKE_INSTALL_MANIFEST}" "${file}\n")
ENDFOREACH(file)
