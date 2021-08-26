package io.deephaven.grpc_api.runner;

class DockerEmpty {
}

// empty on purpose, otherwise
// Execution failed for task ':grpc-api-docker:dockerBuildImage'.
// > Could not build image: COPY failed: file not found in build context or excluded by .dockerignore: stat classes:
// file does not exist
// TODO: exclude the COPY classes part for dockerBuildImage
