#!/bin/bash

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
command -v mvn >/dev/null 2>&1 || { echo >&2 "mvn is required to run $0."; exit 1; }

source $HONEYCOMB_HOME/scripts/utilities/constants.sh
echo -e "Building memory adaptor leiningen project\n"

cd $HONEYCOMB_HOME/memory
lein do clean, check, compile, jar, install
