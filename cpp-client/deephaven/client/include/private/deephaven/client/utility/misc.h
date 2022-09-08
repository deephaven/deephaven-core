/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <map>
#include <memory>
#include <vector>
#include <arrow/type.h>

namespace deephaven::client::utility {
class ColumnDefinitions {
public:
  typedef std::map<std::string, std::shared_ptr<arrow::DataType>> map_t;
  typedef std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>>
      vec_t;

  ColumnDefinitions(vec_t vec, map_t map);
  ~ColumnDefinitions();

  const vec_t &vec() const { return vec_; }

  const map_t &map() const { return map_; }

  size_t size() const { return vec_.size(); }

private:
  vec_t vec_;
  map_t map_;
};
}  // namespace deephaven::client::utility
