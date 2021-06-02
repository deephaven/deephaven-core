#pragma once

#include <string>
#include <string_view>

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
class EscapeUtils {
public:
  static std::string escapeJava(std::string_view s);
  static void appendEscapedJava(std::string_view s, std::string *dest);
};
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
