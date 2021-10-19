cmake_minimum_required(VERSION 3.16)

if(TARGET client)
    # The library has already been built (i.e. through some
    # other path in the build graph).
    return()
endif()

project(deephaven)

set(CMAKE_CXX_STANDARD 17)

# for CMAKE_INSTALL_{dir}
include(GNUInstallDirs)

add_subdirectory(client)

install(DIRECTORY client/include/public/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

install(TARGETS client
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
