#!/bin/sh

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
command -v astyle >/dev/null 2>&1 || { echo >&2 "astyle is required to run $0."; exit 1; }

cd $HONEYCOMB_HOME/honeycomb
astyle -A2 -s2 -n -K -p -xd -j -c -S -C --align-pointer=type --align-reference=type *.cc *.h
