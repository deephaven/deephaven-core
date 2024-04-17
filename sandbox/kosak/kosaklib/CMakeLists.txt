# CMakeList.txt : CMake project for kosaklib, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_STANDARD 17)

# Enable Hot Reload for MSVC compilers if supported.
if (POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
endif()

project ("kosaklib")

# Add source to this project's executable.
add_library (kosaklib SHARED "kosaklib.cc" "kosaklib.h")

if (LINUX)
  target_compile_options(kosaklib PRIVATE -fms-extensions)
endif()

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET kosaklib PROPERTY CXX_STANDARD 20)
endif()

# TODO: Add tests and install targets if needed.
