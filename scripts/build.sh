#!/bin/bash

# Exit the shell when a command exits with a non-zero value and it has not been checked
set -e

: ${HONEYCOMB_HOME?"Need to set the HONEYCOMB_HOME environment variable to the top of the project."}

# Set the location of the build scripts
BUILD_SCRIPTS_DIR=$HONEYCOMB_HOME/scripts/utilities

# Initialize default behavior
export MAVEN_TEST_MODE="ut"
export DEV_MODE=false

usage ()
{
    echo "Usage: $0 [OPTION]"
    echo -e "-t\tMaven test runner mode\t(none: no tests, all: all tests, ut: only unit tests, it: only integration tests)"
    echo -e "-d\tDevelopment mode\t builds MySQL in debug mode, installs mtr integrations tests"
    exit 1
}

# Extract the arguments provided to the script
while getopts ":t:d" option
do
  case "${option}" in
        t)
            MAVEN_TEST_MODE=${OPTARG}
            [[ -n "$MAVEN_TEST_MODE" ]] || { echo "You must supply a valid test mode argument "; usage; }
            ;;
        d)
            export DEV_MODE=true
            ;;
        \?)
            echo "Unknown option provided: -$OPTARG" >&2
            usage
            ;;
        :)
            echo "Missing required argument for option: -$OPTARG" >&2
            usage
            ;;
  esac
done

if $DEV_MODE; then
  echo "DEV MODE ACTIVATED"
fi

$BUILD_SCRIPTS_DIR/initial-setup-check.sh
$BUILD_SCRIPTS_DIR/mvn-build-install.sh "$MAVEN_TEST_MODE"
$BUILD_SCRIPTS_DIR/plugin-build-install.sh
$BUILD_SCRIPTS_DIR/mysql-restart.sh
if $DEV_MODE; then
  $BUILD_SCRIPTS_DIR/install-tests.sh
fi
