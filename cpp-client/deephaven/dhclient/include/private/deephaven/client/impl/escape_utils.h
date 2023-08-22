/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string>
#include <string_view>

namespace deephaven::client::impl {
class EscapeUtils {
public:
  [[nodiscard]]
  static std::string EscapeJava(std::string_view s);
  static void AppendEscapedJava(std::string_view s, std::string *dest);
};
}  // namespace deephaven::client::impl
