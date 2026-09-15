cmake_minimum_required(VERSION 3.14)

if(TARGET client)
    # The library has already been built (i.e. through some
    # other path in the build graph).
    return()
endif()

project(deephaven)

set(deephaven_VERSION 43.0)
set(CMAKE_CXX_STANDARD 17)

if(WIN32)
    add_compile_definitions(WIN32_LEAN_AND_MEAN NOMINMAX)
endif()

# for CMAKE_INSTALL_{dir}
include(GNUInstallDirs)

# To enable address sanitizer, add `-DSANITIZE_ADDRESS=ON` to
# the list of options you pass for running cmake.
option(SANITIZE_ADDRESS "Enable address sanitizer" "OFF")
if(${SANITIZE_ADDRESS} STREQUAL "ON")
  add_compile_options("-fsanitize=address")
  link_libraries("-fsanitize=address")
endif()

option(DHCORE_ONLY "Only build dhcore, skip rest" OFF)

add_subdirectory(dhcore)
if(NOT DHCORE_ONLY)
    add_subdirectory(dhclient)
    add_subdirectory(tests)
    add_subdirectory(examples)
endif()

install(DIRECTORY dhcore/include/public/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

if(NOT DHCORE_ONLY)
    install(DIRECTORY dhclient/include/public/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        )

    install(TARGETS dhclient dhcore dhcore_static dhclient_tests
        EXPORT deephavenTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        )

    # Make the install prefix self-contained by copying the vcpkg-built runtime
    # libraries next to ours, so consumers of the prefix (CI artifacts, the
    # enterprise client, ...) need no vcpkg tree of their own. This copies all
    # of vcpkg's shared libraries, not just the ones we link (~65MB extra);
    # trimming would need install(RUNTIME_DEPENDENCY_SET) with per-platform
    # system-library exclusions and is not worth it at this size.
    # VCPKG_INSTALLED_DIR is set by the vcpkg toolchain in every mode: in
    # manifest mode it is ${CMAKE_BINARY_DIR}/vcpkg_installed, but the Docker
    # build points it at a prebuilt tree elsewhere.
    if(VCPKG_TOOLCHAIN)
        if(WIN32)
            install(DIRECTORY ${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/bin/
                DESTINATION ${CMAKE_INSTALL_BINDIR}
                FILES_MATCHING PATTERN "*.dll"
            )
        else()
            install(DIRECTORY ${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/
                DESTINATION ${CMAKE_INSTALL_LIBDIR}
                FILES_MATCHING
                    PATTERN "*.so"
                    PATTERN "*.so.*"
                    PATTERN "pkgconfig" EXCLUDE
                    PATTERN "cmake" EXCLUDE
            )
        endif()
    endif()

      include(CMakePackageConfigHelpers)
      write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/deephaven/deephavenConfigVersion.cmake"
        VERSION ${deephaven_VERSION}
        COMPATIBILITY AnyNewerVersion
        )

      export(EXPORT deephavenTargets
        FILE "${CMAKE_CURRENT_BINARY_DIR}/deephaven/deephavenTargets.cmake"
        NAMESPACE deephaven::
        )
      configure_file(cmake/deephavenConfig.cmake
        "${CMAKE_CURRENT_BINARY_DIR}/deephaven/deephavenConfig.cmake"
        COPYONLY
        )

      set(ConfigPackageLocation ${CMAKE_INSTALL_LIBDIR}/cmake/deephaven)
      install(EXPORT deephavenTargets
        FILE
        deephavenTargets.cmake
        NAMESPACE
        deephaven::
        DESTINATION
        ${ConfigPackageLocation}
        )
      install(
        FILES
        cmake/deephavenConfig.cmake
        "${CMAKE_CURRENT_BINARY_DIR}/deephaven/deephavenConfigVersion.cmake"
        DESTINATION
        ${ConfigPackageLocation}
        COMPONENT
        Devel
        )

      # Publish our vcpkg manifest next to the CMake package config so that
      # downstream consumers (e.g. the enterprise client) can verify at their
      # configure time that they build with the same vcpkg baseline and
      # version overrides. Mismatched pins mean the two projects install
      # incompatible copies of shared third-party DLLs (abseil, protobuf,
      # arrow, ...) into the same prefix, which fails at load time with
      # "Entry Point Not Found".
      install(
        FILES
        vcpkg.json
        DESTINATION
        ${ConfigPackageLocation}
        COMPONENT
        Devel
        )

endif()
