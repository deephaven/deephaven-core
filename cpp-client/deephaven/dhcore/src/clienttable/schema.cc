/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/clienttable/schema.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <string>
#include <utility>
#include <vector>

#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"

using deephaven::dhcore::utility::MakeReservedVector;

namespace deephaven::dhcore::clienttable {
namespace {
std::map<std::string_view, size_t, std::less<>> ValidateAndMakeIndex(
    const std::vector<std::string> &names, const std::vector<ElementType> &types) {
  if (names.size() != types.size()) {
    auto message = fmt::format("Sizes differ: {} vs {}", names.size(), types.size());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  std::map<std::string_view, size_t, std::less<>> index;
  for (size_t i = 0; i != names.size(); ++i) {
    std::string_view sv_name = names[i];
    auto [ip, inserted] = index.insert(std::make_pair(sv_name, i));
    if (!inserted) {
      auto message = fmt::format("Duplicate column name: {}", sv_name);
      throw std::runtime_error(message);
    }
  }
  return index;
}
}  // namespace

std::shared_ptr<Schema> Schema::Create(std::vector<std::string> names,
    std::vector<ElementType> types) {
  auto type_ids = MakeReservedVector<ElementTypeId::Enum>(types.size());
  for (const auto &type : types) {
    type_ids.push_back(type.Id());
  }
  auto index = ValidateAndMakeIndex(names, types);

  return std::make_shared<Schema>(Private(), std::move(names), std::move(types),
    std::move(index));
}

Schema::Schema(Private, std::vector<std::string> names, std::vector<ElementType> types,
     std::map<std::string_view, size_t, std::less<>> index) : names_(std::move(names)),
     types_(std::move(types)), index_(std::move(index)) {
}

Schema::~Schema() = default;

std::optional<int32_t> Schema::GetColumnIndex(std::string_view name, bool strict) const {
  auto ip = index_.find(name);
  if (ip != index_.end()) {
    return static_cast<int32_t>(ip->second);
  }
  // Not found: check strictness flag.
  if (!strict) {
    return {};
  }
  auto message = fmt::format(R"(Column name "{}" not found)", name);
  throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
}
}  // namespace deephaven::dhcore::clienttable
