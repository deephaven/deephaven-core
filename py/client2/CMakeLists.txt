cmake_minimum_required(VERSION 3.16)
project(pydeephaven2)

# Running from a Python sdist tarball
set(LOCAL_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/cmake_modules")
if(EXISTS "${LOCAL_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${LOCAL_CMAKE_MODULES})
endif()

set(Python3_FIND_STRATEGY "LOCATION")
set(Python3_FIND_REGISTRY "LAST")
set(Python3_FIND_FRAMEWORK "LAST")

# Allow "make install" to not depend on all targets.
#
# Must be declared in the top-level CMakeLists.txt.
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

set(CMAKE_MACOSX_RPATH 1)
if(DEFINED ENV{MACOSX_DEPLOYMENT_TARGET})
  set(CMAKE_OSX_DEPLOYMENT_TARGET $ENV{MACOSX_DEPLOYMENT_TARGET})
else()
  set(CMAKE_OSX_DEPLOYMENT_TARGET 10.9)
endif()

include(BuildUtils)

# Cython generated code emits way to many warnings at CHECKIN and EVERYTHING
set(BUILD_WARNING_LEVEL "PRODUCTION")

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
  set(BUILD_OUTPUT_ROOT_DIRECTORY
      "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}")
  # Link build/latest to the current build directory, to avoid developers
  # accidentally running the latest debug build when in fact they're building
  # release builds.
  file(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
  if(NOT APPLE)
    set(MORE_ARGS "-T")
  endif()
  execute_process(COMMAND ln ${MORE_ARGS} -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
                          ${CMAKE_CURRENT_BINARY_DIR}/build/latest)
else()
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}")
endif()

message(STATUS "Generator: ${CMAKE_GENERATOR}")
message(STATUS "Build output directory: ${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# Python and Numpy libraries
find_package(Python3Alt REQUIRED)
include(UseCython)

include_directories(SYSTEM ${NUMPY_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS} src)
include_directories(SYSTEM ../../cpp-client/deephaven/client/include/public)

#find_package(deephaven REQUIRED)
add_subdirectory(../../cpp-client/deephaven deephaven_dir)

# Always bundle includes
#get_filename_component(DEEPHAVEN_INCLUDE_REALPATH "../../cpp-client/deephaven/client/include" REALPATH)
#file(COPY ${DEEPHAVEN_INCLUDE_REALPATH} DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY}/include)

if(UNIX)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
endif()

set(CYTHON_EXTENSIONS
  # Add here to list as more pxd wrapping is done.
  pydeephaven2)

foreach(module ${CYTHON_EXTENSIONS})
  string(REPLACE "." ";" directories ${module})
  list(GET directories -1 module_name)
  list(REMOVE_AT directories -1)

  string(REPLACE "." "/" module_root "${module}")
  set(module_SRC ${CMAKE_PROJECT_NAME}/${module_root}.pyx)
  set_source_files_properties(${module_SRC} PROPERTIES CYTHON_IS_CXX 1)

  cython_add_module(${module_name} ${module_name}_pyx ${module_name}_output ${module_SRC})

  if(directories)
    string(REPLACE ";" "/" module_output_directory ${directories})
    set_target_properties(${module_name} PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                                    ${module_output_directory})
  endif()

  target_link_libraries(${module_name} PRIVATE ${LINK_LIBS})

  # Generated files will be moved to the right directory by setup.py.
endforeach(module)
