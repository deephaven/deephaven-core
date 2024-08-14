include(CMakeFindDependencyMacro)
find_dependency(Arrow 16.0.0)
find_dependency(ArrowFlight 16.0.0)
find_dependency(Immer)
find_dependency(Protobuf)
find_dependency(gRPC)
find_dependency(Threads)

include("${CMAKE_CURRENT_LIST_DIR}/deephavenTargets.cmake")
