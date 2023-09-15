cmake_minimum_required(VERSION 3.14)

if(TARGET client)
    # The library has already been built (i.e. through some
    # other path in the build graph).
    return()
endif()

project(deephaven)

set(CMAKE_CXX_STANDARD 17)

# for CMAKE_INSTALL_{dir}
include(GNUInstallDirs)

add_compile_options(-Wall -Werror -Wno-deprecated-declarations)

# To enable address sanitizer, add `-DSANITIZE_ADDRESS=ON` to
# the list of options you pass for running cmake.
option(SANITIZE_ADDRESS "Enable address sanitizer" "OFF")
if(${SANITIZE_ADDRESS} STREQUAL "ON")
  add_compile_options("-fsanitize=address")
  link_libraries("-fsanitize=address")
endif()

add_subdirectory(dhclient)
add_subdirectory(dhcore)
add_subdirectory(tests)
add_subdirectory(examples)

install(DIRECTORY dhclient/include/public/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

install(DIRECTORY dhcore/include/public/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

install(TARGETS dhclient dhcore
    EXPORT deephavenConfig
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

install(EXPORT deephavenConfig
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/deephaven
    NAMESPACE deephaven::
    )
