# Install script for directory: /Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man

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

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "ManPages")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/man/man1" TYPE FILE FILES
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/comp_err.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/innochecksum.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/msql2mysql.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/my_print_defaults.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/myisam_ftdump.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/myisamchk.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/myisamlog.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/myisampack.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql-stress-test.pl.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql-test-run.pl.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql.server.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_client_test.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_client_test_embedded.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_config.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_convert_table_format.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_find_rows.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_fix_extensions.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_install_db.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_plugin.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_secure_installation.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_setpermission.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_tzinfo_to_sql.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_upgrade.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_waitpid.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysql_zap.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqlaccess.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqladmin.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqlbinlog.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqlbug.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqlcheck.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqld_multi.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqld_safe.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqldump.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqldumpslow.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqlhotcopy.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqlimport.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqlman.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqlshow.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqlslap.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqltest.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqltest_embedded.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/perror.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/replace.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/resolve_stack_dump.1"
    "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/resolveip.1"
    )
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "ManPages")

IF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "ManPages")
  FILE(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/man/man8" TYPE FILE FILES "/Users/showell/Development/mysql-cloud-engine/mysql-5.5.25a/man/mysqld.8")
ENDIF(NOT CMAKE_INSTALL_COMPONENT OR "${CMAKE_INSTALL_COMPONENT}" STREQUAL "ManPages")

