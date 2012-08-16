#!/bin/sh

sudo mysql.server stop && sudo mysql.server start --debug=d:t:L:F:o,/tmp/mysqld.trace
