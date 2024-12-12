cmake_minimum_required(VERSION 3.14)

if(TARGET client)
    # The library has already been built (i.e. through some
    # other path in the build graph).
    return()
endif()

project(deephaven)

set(deephaven_VERSION 0.37.1)
set(CMAKE_CXX_STANDARD 17)

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
      
endif()
