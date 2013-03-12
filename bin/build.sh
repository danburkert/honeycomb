#!/bin/bash

# Exit the shell when a command exits with a non-zero value and it has not been checked
set -e

: ${HONEYCOMB_HOME?"Need to set the HONEYCOMB_HOME environment variable to the top of the project."}

# Set the location of the build scripts
BUILD_SCRIPTS_DIR=$HONEYCOMB_HOME/bin

$BUILD_SCRIPTS_DIR/initial-setup-check.sh
$BUILD_SCRIPTS_DIR/mvn-build-install.sh
$BUILD_SCRIPTS_DIR/plugin-build-install.sh
$BUILD_SCRIPTS_DIR/mysql-restart.sh
$BUILD_SCRIPTS_DIR/install-tests.sh
