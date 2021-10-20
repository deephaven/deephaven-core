cmake_minimum_required(VERSION 3.16)
project(build-all-examples)

add_subdirectory(../cleanup cleanup_dir)
add_subdirectory(../create_table_with_arrow_flight create_table_with_arrow_flight_dir)
add_subdirectory(../create_table_with_table_maker create_table_with_table_maker_dir)
add_subdirectory(../hello_world hello_world_dir)
add_subdirectory(../readcsv readcsv_dir)
add_subdirectory(../read_table_with_arrow_flight read_table_with_arrow_flight_dir)
