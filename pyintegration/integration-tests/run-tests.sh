#!/bin/bash
set -eux
for test_file_name in  ./*test.py;
do
   echo $test_file_name
   python3 $test_file_name>> /out/report/scripting-test_results 2>&1
done
