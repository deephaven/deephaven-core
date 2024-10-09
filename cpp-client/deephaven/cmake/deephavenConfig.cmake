include(CMakeFindDependencyMacro)
find_dependency(Arrow CONFIG REQUIRED)
find_dependency(ArrowFlight CONFIG REQUIRED)
find_dependency(Immer CONFIG REQUIRED)
find_dependency(Protobuf CONFIG REQUIRED)
find_dependency(gRPC CONFIG REQUIRED)
find_dependency(Threads REQUIRED)

include("${CMAKE_CURRENT_LIST_DIR}/deephavenTargets.cmake")
