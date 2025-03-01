cmake_minimum_required(VERSION 3.14)
project(dhcore)

set(CMAKE_CXX_STANDARD 17)

# for CMAKE_INSTALL_{dir}
include(GNUInstallDirs)

find_package(Immer CONFIG REQUIRED)

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
    src/interop/testapi/basic_interop_interactions.cc
    src/interop/interop_util.cc
    src/interop/utility_interop.cc
    src/ticking/barrage_processor.cc
    src/ticking/immer_table_state.cc
    src/ticking/index_decoder.cc
    src/ticking/shift_processor.cc
    src/ticking/space_mapper.cc
    src/ticking/ticking.cc
    src/utility/cython_support.cc
    src/utility/utility.cc
    src/utility/utility_platform_specific.cc

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
    include/public/deephaven/dhcore/interop/testapi/basic_interop_interactions.h
    include/public/deephaven/dhcore/interop/interop_util.h
    include/public/deephaven/dhcore/interop/utility_interop.h
    include/public/deephaven/dhcore/ticking/barrage_processor.h
    include/public/deephaven/dhcore/ticking/ticking.h
    include/public/deephaven/dhcore/utility/cython_support.h
    include/public/deephaven/dhcore/utility/utility.h

    flatbuf/deephaven/flatbuf/Barrage_generated.h

    # we vendor flatbuffers but it is private to us
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

    # The way we vendor the "fmt" library is we compile the cc files directly into dhcore,
    # and we publish the .h files at deephaven/third_party/fmt
    third_party/fmt/src/format.cc
    third_party/fmt/src/os.cc

    include/public/deephaven/third_party/fmt/args.h
    include/public/deephaven/third_party/fmt/chrono.h
    include/public/deephaven/third_party/fmt/color.h
    include/public/deephaven/third_party/fmt/compile.h
    include/public/deephaven/third_party/fmt/core.h
    include/public/deephaven/third_party/fmt/format-inl.h
    include/public/deephaven/third_party/fmt/format.h
    include/public/deephaven/third_party/fmt/os.h
    include/public/deephaven/third_party/fmt/ostream.h
    include/public/deephaven/third_party/fmt/printf.h
    include/public/deephaven/third_party/fmt/ranges.h
    include/public/deephaven/third_party/fmt/std.h
    include/public/deephaven/third_party/fmt/xchar.h

    # we vendor the date library also but it's private to us, so we don't export its .h file
    third_party/date/include/date/date.h
)

#
# shared and static libraries built from the same object files
# https://stackoverflow.com/questions/2152077/is-it-possible-to-get-cmake-to-build-both-a-static-and-shared-library-at-the-sam
# (see second answer, "Since CMake version 2.8.8, you can ...")
# https://stackoverflow.com/questions/38832528/transitive-target-include-directories-on-object-libraries
#

add_library(dhcore SHARED ${ALL_FILES})
add_library(dhcore_static STATIC ${ALL_FILES})

# This is so deephaven::dhcore works both when using the installed CMake config
# and when using this project as a CMake subdirectory of your own project.
add_library(deephaven::dhcore ALIAS dhcore)

if (WIN32)
  set_property(TARGET dhcore PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif()

foreach (whichlib dhcore dhcore_static)
  set_property(TARGET ${whichlib} PROPERTY POSITION_INDEPENDENT_CODE ON)

  if (LINUX)
    target_compile_options(${whichlib} PRIVATE -Wall -Werror -Wno-deprecated-declarations)
  endif()

  if (WIN32)
    # /Wall is a bit too chatty so we stick with /W3
    # /bigobj needed because ticking/immer_table_state.cc compiles to something too large apparently
    target_compile_options(${whichlib} PRIVATE /W3 /bigobj)

    target_compile_definitions(${whichlib} PRIVATE _SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING)
    target_link_libraries(${whichlib} PRIVATE ws2_32)
  endif()

  target_include_directories(${whichlib} PRIVATE include/private)
  target_include_directories(${whichlib} PRIVATE third_party/date/include)
  target_include_directories(${whichlib} PRIVATE third_party/flatbuffers/include)
  target_include_directories(${whichlib} PRIVATE third_party/roaring/include)
  target_include_directories(${whichlib} PRIVATE flatbuf)
  target_include_directories(${whichlib} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/public>)

  target_link_libraries(${whichlib} PRIVATE immer)
endforeach()
