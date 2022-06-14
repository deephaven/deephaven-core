/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string_view>
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::highlevel::impl {

#if defined(__clang__)
#define DEEPHAVEN_PRETTY_FUNCTION __PRETTY_FUNCTION__
#elif defined(__GNUC__)
#define DEEPHAVEN_PRETTY_FUNCTION __PRETTY_FUNCTION__
#elif defined(__MSC_VER)
#define DEEPHAVEN_PRETTY_FUNCTION __FUNCSIG__
#else
# error Unsupported compiler
#endif

// https://stackoverflow.com/questions/281818/unmangling-the-result-of-stdtype-infoname
template <typename T>
constexpr std::string_view getTypeName() {
#if defined(__clang__)
  constexpr auto prefix = std::string_view{"[T = "};
  constexpr auto suffix = "]";
#elif defined(__GNUC__)
  constexpr auto prefix = std::string_view{"with T = "};
  constexpr auto suffix = "; ";
#elif defined(__MSC_VER)
  constexpr auto prefix = std::string_view{"get_type_name<"};
  constexpr auto suffix = ">(void)";
#else
# error Unsupported compiler
#endif

  constexpr auto function = std::string_view{DEEPHAVEN_PRETTY_FUNCTION};

  const auto start = function.find(prefix) + prefix.size();
  const auto end = function.find(suffix);
  const auto size = end - start;

  return function.substr(start, size);
}

template<typename DESTP, typename SRCP>
DESTP verboseCast(std::string_view caller, SRCP ptr) {
  using deephaven::client::utility::stringf;

  auto *typedPtr = dynamic_cast<DESTP>(ptr);
  if (typedPtr != nullptr) {
    return typedPtr;
  }
  typedef decltype(*std::declval<DESTP>()) destType_t;
  auto message = stringf("%o: Expected type %o. Got type %o",
      caller, getTypeName<destType_t>(), typeid(*ptr).name());
  throw std::runtime_error(message);
}
}  // namespace deephaven::client::highlevel::impl
