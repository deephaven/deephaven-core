#!/bin/bash
set -eux

#
# Run each test py file in its own interpreter so they don't share globals
#

DIR=$(dirname "$0")

for test_file_name in $DIR/*test.py;
do
   echo $test_file_name >> /out/report/scripting-test_results 2>&1
   python3 $test_file_name >> /out/report/scripting-test_results 2>&1
done
