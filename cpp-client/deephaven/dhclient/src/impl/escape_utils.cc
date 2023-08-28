/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/escape_utils.h"

#include <codecvt>
#include <locale>
#include <string>

namespace deephaven::client {
namespace impl {
std::string EscapeUtils::EscapeJava(std::string_view s) {
  std::string result;
  AppendEscapedJava(s, &result);
  return result;
}

void EscapeUtils::AppendEscapedJava(std::string_view s, std::string *dest) {
  typedef std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter_t;
  std::u16string u16s = converter_t().from_bytes(s.begin(), s.end());

  for (auto u16ch: u16s) {
    switch (u16ch) {
      case '\b':
        dest->append("\\b");
        continue;
      case '\f':
        dest->append("\\f");
        continue;
      case '\n':
        dest->append("\\n");
        continue;
      case '\r':
        dest->append("\\r");
        continue;
      case '\t':
        dest->append("\\t");
        continue;
      case '"':
      case '\'':
      case '\\':
        dest->push_back('\\');
        // The cast is to silence Clang-Tidy.
        dest->push_back(static_cast<char>(u16ch));
        continue;
      default:
        break;
    }

    if (u16ch < 32 || u16ch > 0x7f) {
      char buffer[16];  // plenty
      snprintf(buffer, sizeof(buffer), "\\u%04x", u16ch);
      dest->append(buffer);
      continue;
    }
    // The cast is to silence Clang-Tidy.
    dest->push_back(static_cast<char>(u16ch));
  }
}
}  // namespace impl
}  // namespace deephaven::client
