#!/bin/sh

CSCOPEFILE=cscope.files
rm $CSCOPEFILE
find . -name '*.c' > $CSCOPEFILE
find . -name '*.cc' >> $CSCOPEFILE
find . -name '*.h' >> $CSCOPEFILE

cscope -b
CSCOPE_DB=~/Development/mysql-5.5.25a/cscope.out
export CSCOPE_DB
ctags -R .
