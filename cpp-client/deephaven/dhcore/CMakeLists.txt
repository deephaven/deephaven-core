cmake_minimum_required(VERSION 3.14)
project(dhcore)

set(CMAKE_CXX_STANDARD 17)

# for CMAKE_INSTALL_{dir}
include(GNUInstallDirs)

find_package(Immer REQUIRED)

set(ALL_FILES
    src/types.cc
    src/chunk/chunk.cc
    src/chunk/chunk_maker.cc
    src/clienttable/schema.cc
    src/clienttable/client_table.cc
    src/column/array_column_source.cc
    src/column/column_source.cc
    src/column/column_source_helpers.cc
    src/column/column_source_utils.cc
    src/container/row_sequence.cc
    src/immerutil/abstract_flex_vector.cc
    src/immerutil/immer_column_source.cc
    src/ticking/barrage_processor.cc
    src/ticking/immer_table_state.cc
    src/ticking/index_decoder.cc
    src/ticking/shift_processor.cc
    src/ticking/space_mapper.cc
    src/ticking/ticking.cc
        src/utility/cython_support.cc
    src/utility/utility.cc

    include/private/deephaven/dhcore/ticking/immer_table_state.h
    include/private/deephaven/dhcore/ticking/index_decoder.h
    include/private/deephaven/dhcore/ticking/shift_processor.h
    include/private/deephaven/dhcore/ticking/space_mapper.h
    include/private/deephaven/dhcore/ticking/subscription_handle.h
    include/public/deephaven/dhcore/types.h
    include/public/deephaven/dhcore/chunk/chunk.h
    include/public/deephaven/dhcore/chunk/chunk_maker.h
    include/public/deephaven/dhcore/chunk/chunk_traits.h
    include/public/deephaven/dhcore/clienttable/schema.h
    include/public/deephaven/dhcore/clienttable/client_table.h
    include/public/deephaven/dhcore/column/array_column_source.h
    include/public/deephaven/dhcore/column/buffer_column_source.h
    include/public/deephaven/dhcore/column/column_source.h
    include/public/deephaven/dhcore/column/column_source_helpers.h
    include/public/deephaven/dhcore/column/column_source_utils.h
    include/public/deephaven/dhcore/container/row_sequence.h
    include/public/deephaven/dhcore/ticking/barrage_processor.h
    include/public/deephaven/dhcore/ticking/ticking.h
        include/public/deephaven/dhcore/utility/cython_support.h
    include/public/deephaven/dhcore/utility/utility.h

    flatbuf/deephaven/flatbuf/Barrage_generated.h

    third_party/flatbuffers/include/flatbuffers/allocator.h
    third_party/flatbuffers/include/flatbuffers/array.h
    third_party/flatbuffers/include/flatbuffers/base.h
    third_party/flatbuffers/include/flatbuffers/buffer.h
    third_party/flatbuffers/include/flatbuffers/buffer_ref.h
    third_party/flatbuffers/include/flatbuffers/default_allocator.h
    third_party/flatbuffers/include/flatbuffers/detached_buffer.h
    third_party/flatbuffers/include/flatbuffers/flatbuffer_builder.h
    third_party/flatbuffers/include/flatbuffers/flatbuffers.h
    third_party/flatbuffers/include/flatbuffers/stl_emulation.h
    third_party/flatbuffers/include/flatbuffers/string.h
    third_party/flatbuffers/include/flatbuffers/struct.h
    third_party/flatbuffers/include/flatbuffers/table.h
    third_party/flatbuffers/include/flatbuffers/util.h
    third_party/flatbuffers/include/flatbuffers/vector_downward.h
    third_party/flatbuffers/include/flatbuffers/vector.h
    third_party/flatbuffers/include/flatbuffers/verifier.h

    third_party/roaring/include/roaring/roaring.c
    )

add_library(dhcore ${ALL_FILES})
# In order to make a shared library suitable for Cython.
set_property(TARGET dhcore PROPERTY POSITION_INDEPENDENT_CODE ON)

# This is so deephaven::dhcore works both when using the installed CMake config
# and when using this project as a CMake subdirectory of your own project.
add_library(deephaven::dhcore ALIAS dhcore)
target_compile_options(dhcore PRIVATE -Wall -Werror -Wno-deprecated-declarations)

target_include_directories(dhcore PRIVATE include/private)
target_include_directories(dhcore PRIVATE third_party/flatbuffers/include)
target_include_directories(dhcore PRIVATE third_party/roaring/include)
target_include_directories(dhcore PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/public>)

# Protos and flatbuf are doing their own thing.
target_include_directories(dhcore PRIVATE "./proto")
target_include_directories(dhcore PRIVATE "./flatbuf")

target_link_libraries(dhcore PUBLIC immer)
