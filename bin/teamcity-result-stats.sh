#!/bin/sh

test_name=$1
build_number=$2
output=/tmp/$test_name.$build_number.out
./run-$test_name | tee $output
ruby ./parse-test-output.rb $output /tmp/$test_name.csv
rm $output
Rscript ./analyze-tests.R /tmp/$test_name.csv /tmp/$test_name.pdf $build_number
