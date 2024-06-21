/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#define CATCH_CONFIG_RUNNER
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using deephaven::client::tests::GlobalEnvironmentForTests;

int main(int argc, char *argv[], char **envp) {
  // Process envp so we don't have to call getenv(), which Windows complains about.
  GlobalEnvironmentForTests::Init(envp);
  return Catch::Session().run(argc, argv);
}
