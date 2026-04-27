# These settings are inspired by [vcpkg repo]/triplets/x64-linux.cmake

# These settings are the same:
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# This setting is changed because we want dynamic library linking
# rather than static.
set(VCPKG_LIBRARY_LINKAGE dynamic)

# This setting is new because we want to build only the 'release'
# version of the packages, rather the both 'debug' and 'release'
set(VCPKG_BUILD_TYPE release)
