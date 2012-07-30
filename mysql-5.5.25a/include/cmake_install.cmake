# Install script for directory: /Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include

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

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Development")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include" TYPE FILE FILES
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysql.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysql_com.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysql_time.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_list.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_alloc.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/typelib.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysql/plugin.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysql/plugin_audit.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysql/plugin_ftparser.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_dbug.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/m_string.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_sys.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_xml.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysql_embed.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_pthread.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/decimal.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/errmsg.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_global.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_net.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_getopt.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/sslopt-longopts.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_dir.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/sslopt-vars.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/sslopt-case.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/sql_common.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/keycache.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/m_ctype.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_attribute.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_compiler.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysql_version.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/my_config.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysqld_ername.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysqld_error.h"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/sql_state.h"
    )
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Development")

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Development")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/mysql" TYPE DIRECTORY FILES "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/include/mysql/" FILES_MATCHING REGEX "/[^/]*\\.h$")
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "Development")

