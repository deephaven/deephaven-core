cmake_minimum_required(VERSION 3.16)
project(build-all)

add_subdirectory("../deephaven" "deephaven_dir")
add_subdirectory("../tests" "tests_dir")
add_subdirectory("../../cpp-examples/build-all-examples" "examples_dir")
