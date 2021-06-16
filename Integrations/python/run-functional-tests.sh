#!/bin/bash
set -eux
for python_file_name in $(find functional-tests -name *test.py)
do
   python3 $python_file_name >> /out/report/functional-test_results 2>&1
done