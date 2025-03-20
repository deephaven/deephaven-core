project(demos)

set(CMAKE_CXX_STANDARD 17)

set(MAINS
    chapter1
    chapter2
    chapter3
    feedtimes
)

foreach (main ${MAINS})
  add_executable(${main} ${main}.cc)
  target_link_libraries(${main} deephaven::client)
endforeach()
